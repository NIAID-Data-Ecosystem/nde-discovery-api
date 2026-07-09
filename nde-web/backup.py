"""Daily Elasticsearch backup for NDE user profiles."""

import io
import json
import logging
import tempfile
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

BACKUP_FILENAME_PREFIX = "nde_user_profiles_backup"
S3_BUCKET = "nde"
S3_PREFIX = "es_backup"
ES_REQUEST_TIMEOUT = 60


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def _now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0)


def _backup_filename():
    timestamp = _now_utc().strftime("%Y%m%dT%H%M%SZ")
    return f"{BACKUP_FILENAME_PREFIX}_{timestamp}.zip"


def _response_body(response):
    return getattr(response, "body", response)


def build_es_client(config, request_timeout=ES_REQUEST_TIMEOUT):
    """Create a synchronous Elasticsearch client for the production cluster."""
    from elasticsearch import Elasticsearch

    client_kwargs = dict(getattr(config, "ES_ARGS", {}) or {})
    client_kwargs["request_timeout"] = request_timeout
    return Elasticsearch(config.ES_HOST, **client_kwargs)


def _iter_index_docs(client, *, index):
    from elasticsearch import helpers

    yield from helpers.scan(
        client,
        index=index,
        query={"query": {"match_all": {}}},
        size=500,
        scroll="5m",
    )


def _backup_doc(hit):
    doc = {
        "_id": hit.get("_id"),
        "_source": hit.get("_source") or {},
    }
    if hit.get("_routing") is not None:
        doc["_routing"] = hit["_routing"]
    return doc


def backup_user_index(client, index):
    """Return settings, mappings, aliases, and documents for the user index."""
    logger.info("Backing up Elasticsearch user index %s", index)
    index_metadata = _response_body(client.indices.get(index=index))
    data = {}
    doc_count = 0

    for index_name, metadata in index_metadata.items():
        docs = [_backup_doc(hit) for hit in _iter_index_docs(client, index=index_name)]
        data[index_name] = {
            "aliases": metadata.get("aliases", {}),
            "mappings": metadata.get("mappings", {}),
            "settings": metadata.get("settings", {}),
            "docs": docs,
        }
        doc_count += len(docs)
        logger.info("Backed up %s docs from %s", len(docs), index_name)

    return data, doc_count


def write_backup_zip(data, *, local_dir, filename=None):
    """Write backup data to a compressed JSON archive and return its path."""
    backup_dir = Path(local_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)
    archive_path = backup_dir / (filename or _backup_filename())
    json_name = archive_path.with_suffix(".json").name

    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zfile:
        zfile.writestr(
            json_name,
            json.dumps(data, indent=2, default=json_serial, sort_keys=True),
        )
    return archive_path


def upload_to_s3(archive_path, *, bucket, prefix, s3_client=None):
    """Upload an archive to S3 and return the object key."""
    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3")

    key_parts = [part.strip("/") for part in (prefix, Path(archive_path).name) if part]
    obj_key = "/".join(key_parts)
    logger.info("Uploading %s to s3://%s/%s", archive_path, bucket, obj_key)
    s3_client.upload_file(str(archive_path), bucket, obj_key)
    return obj_key


def _backup_key(filename, prefix=S3_PREFIX):
    key_parts = [part.strip("/") for part in (prefix, filename) if part]
    return "/".join(key_parts)


def _latest_backup_key(s3_client, *, bucket=S3_BUCKET, prefix=S3_PREFIX):
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix.strip("/") + "/",
    )
    objects = [
        obj
        for obj in response.get("Contents", [])
        if obj.get("Key", "").endswith((".zip", ".json"))
    ]
    if not objects:
        raise FileNotFoundError(f"No backup objects found in s3://{bucket}/{prefix}/")
    return max(objects, key=lambda obj: obj["LastModified"])["Key"]


def _normalize_backup_key(filename, prefix=S3_PREFIX):
    if not filename:
        return None
    prefix = prefix.strip("/")
    if filename.startswith(prefix + "/"):
        return filename
    return _backup_key(filename, prefix=prefix)


def _read_backup_payload(raw, filename):
    if filename.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(raw)) as zfile:
            json_file = next(
                (name for name in zfile.namelist() if name.endswith(".json")),
                None,
            )
            if not json_file:
                raise ValueError("No JSON file found inside the ZIP archive.")
            with zfile.open(json_file) as json_data:
                return json.load(json_data)
    if filename.endswith(".json"):
        return json.loads(raw)
    raise ValueError("Unsupported backup file type.")


def read_backup_from_s3(
    filename=None,
    *,
    bucket=S3_BUCKET,
    prefix=S3_PREFIX,
    s3_client=None,
):
    """Download and parse a user-profile backup from S3."""
    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3")

    key = _normalize_backup_key(filename, prefix=prefix)
    if not key:
        key = _latest_backup_key(s3_client, bucket=bucket, prefix=prefix)

    logger.info("GET s3://%s/%s", bucket, key)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return key, _read_backup_payload(obj["Body"].read(), key)


def _restorable_index_settings(settings):
    """Remove index settings Elasticsearch will not accept on create."""
    settings = dict(settings or {})
    index_settings = dict(settings.get("index", {}))
    for key in (
        "creation_date",
        "creation_date_string",
        "provided_name",
        "uuid",
        "version",
    ):
        index_settings.pop(key, None)
    if index_settings:
        settings["index"] = index_settings
    else:
        settings.pop("index", None)
    return settings


def _create_index_from_backup(client, *, index, index_data):
    body = {
        "settings": _restorable_index_settings(index_data.get("settings", {})),
        "mappings": index_data.get("mappings", {}),
    }
    aliases = index_data.get("aliases")
    if aliases:
        body["aliases"] = aliases
    client.indices.create(index=index, **body)


def restore_user_index(client, backup_data, target_index, *, replace_index=False):
    """Restore backed-up user profile documents into an Elasticsearch index."""
    if not backup_data:
        raise ValueError("No backup data provided.")

    source_index, index_data = next(iter(backup_data.items()))
    if replace_index and client.indices.exists(index=target_index):
        client.indices.delete(index=target_index)

    if not client.indices.exists(index=target_index):
        _create_index_from_backup(client, index=target_index, index_data=index_data)

    docs = index_data.get("docs") or []
    if not docs:
        return {
            "source_index": source_index,
            "target_index": target_index,
            "docs_restored": 0,
            "errors": 0,
        }

    from elasticsearch import helpers

    actions = []
    for doc in docs:
        action = {
            "_op_type": "index",
            "_index": target_index,
            "_id": doc["_id"],
            "_source": doc.get("_source") or {},
        }
        if doc.get("_routing") is not None:
            action["_routing"] = doc["_routing"]
        actions.append(action)

    restored, errors = helpers.bulk(
        client,
        actions,
        refresh=True,
        stats_only=True,
    )
    return {
        "source_index": source_index,
        "target_index": target_index,
        "docs_restored": restored,
        "errors": errors,
    }


def restore_from_s3(
    config,
    filename=None,
    *,
    bucket=S3_BUCKET,
    prefix=S3_PREFIX,
    client=None,
    s3_client=None,
    target_index=None,
    replace_index=False,
):
    """Restore user profiles from an S3 backup object."""
    client = client or build_es_client(config)
    key, backup_data = read_backup_from_s3(
        filename,
        bucket=bucket,
        prefix=prefix,
        s3_client=s3_client,
    )
    result = restore_user_index(
        client,
        backup_data,
        target_index or config.ES_USER_INDEX,
        replace_index=replace_index,
    )
    result.update({"s3_bucket": bucket, "s3_key": key})
    logger.info("User-profile restore complete: %s", json.dumps(result, sort_keys=True))
    return result


def daily_backup_routine(
    config,
    *,
    client=None,
    s3_client=None,
    bucket=S3_BUCKET,
    prefix=S3_PREFIX,
    local_dir=None,
    raise_on_error=False,
):
    """Create the daily user-profile backup archive and upload it to S3."""
    try:
        client = client or build_es_client(config)
        backup_data, doc_count = backup_user_index(client, config.ES_USER_INDEX)
        if local_dir:
            archive_path = write_backup_zip(backup_data, local_dir=local_dir)
            s3_key = upload_to_s3(
                archive_path,
                bucket=bucket,
                prefix=prefix,
                s3_client=s3_client,
            )
        else:
            with tempfile.TemporaryDirectory(prefix="nde-user-backup-") as tmpdir:
                archive_path = write_backup_zip(backup_data, local_dir=tmpdir)
                s3_key = upload_to_s3(
                    archive_path,
                    bucket=bucket,
                    prefix=prefix,
                    s3_client=s3_client,
                )
        result = {
            "archive": archive_path.name,
            "s3_bucket": bucket,
            "s3_key": s3_key,
            "index": config.ES_USER_INDEX,
            "doc_count": doc_count,
        }
        logger.info(
            "Daily user-profile backup complete: %s",
            json.dumps(result, sort_keys=True),
        )
        return result
    except Exception:
        logger.error("Daily user-profile backup failed", exc_info=True)
        if raise_on_error:
            raise
        return None
