"""Daily Elasticsearch backup for NDE user profiles."""

import json
import logging
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

BACKUP_FILENAME_PREFIX = "nde_user_profiles_backup"
S3_BUCKET = "nde"
S3_PREFIX = "es_backup"
LOCAL_BACKUP_DIR = "."
LOCAL_BACKUPS_TO_KEEP = 10
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


def cleanup_backup_files(*, local_dir, keep_last):
    """Delete older local backup archives, keeping the newest filenames."""
    backup_dir = Path(local_dir)
    backup_files = sorted(
        backup_dir.glob(f"{BACKUP_FILENAME_PREFIX}_*.zip"),
        reverse=True,
    )
    files_to_keep = backup_files[:keep_last]
    files_to_delete = backup_files[keep_last:]
    deleted_count = 0

    for old_file in files_to_delete:
        old_file.unlink(missing_ok=True)
        deleted_count += 1
        logger.info("Deleted old local backup file %s", old_file)

    return len(files_to_keep), deleted_count


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


def daily_backup_routine(
    config,
    *,
    client=None,
    s3_client=None,
    bucket=S3_BUCKET,
    prefix=S3_PREFIX,
    local_dir=LOCAL_BACKUP_DIR,
    keep_last=LOCAL_BACKUPS_TO_KEEP,
    raise_on_error=False,
):
    """Create the daily user-profile backup archive and upload it to S3."""
    try:
        client = client or build_es_client(config)
        backup_data, doc_count = backup_user_index(client, config.ES_USER_INDEX)
        archive_path = write_backup_zip(
            backup_data,
            local_dir=local_dir,
        )
        s3_key = upload_to_s3(
            archive_path,
            bucket=bucket,
            prefix=prefix,
            s3_client=s3_client,
        )
        kept, deleted = cleanup_backup_files(
            local_dir=local_dir,
            keep_last=keep_last,
        )
        result = {
            "archive": str(archive_path),
            "s3_bucket": bucket,
            "s3_key": s3_key,
            "index": config.ES_USER_INDEX,
            "doc_count": doc_count,
            "local_files_kept": kept,
            "local_files_deleted": deleted,
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
