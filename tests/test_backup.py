import importlib.util
import io
import json
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace


BACKUP_PATH = Path(__file__).resolve().parents[1] / "nde-web" / "backup.py"


def _load_backup_module():
    spec = importlib.util.spec_from_file_location("nde_backup", BACKUP_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeIndices:
    def __init__(self):
        self.requests = []

    def get(self, *, index):
        self.requests.append(index)
        return {
            index: {
                "aliases": {"users_alias": {}},
                "mappings": {"properties": {"username": {"type": "keyword"}}},
                "settings": {"index": {"number_of_shards": "1"}},
            }
        }


class FakeClient:
    def __init__(self):
        self.indices = FakeIndices()


class FakeS3Client:
    def __init__(self):
        self.uploads = []
        self.objects = {}
        self.last_modified = {}

    def upload_file(self, filename, bucket, key):
        self.uploads.append({"filename": filename, "bucket": bucket, "key": key})
        self.objects[(bucket, key)] = Path(filename).read_bytes()

    def get_object(self, *, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def list_objects_v2(self, *, Bucket, Prefix):
        contents = []
        for bucket, key in self.objects:
            if bucket == Bucket and key.startswith(Prefix):
                contents.append(
                    {
                        "Key": key,
                        "LastModified": self.last_modified.get(
                            key,
                            datetime(2026, 7, 1, tzinfo=timezone.utc),
                        ),
                    }
                )
        return {"Contents": contents}


def _zip_payload(filename, payload):
    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w", zipfile.ZIP_DEFLATED) as zfile:
        zfile.writestr(filename, json.dumps(payload))
    return raw.getvalue()


def test_backup_user_index_exports_index_metadata_and_docs(monkeypatch):
    module = _load_backup_module()
    client = FakeClient()
    hits = [
        {"_id": "github:alice", "_source": {"username": "alice"}},
        {
            "_id": "orcid:0000-0001",
            "_routing": "orcid",
            "_source": {"username": "0000-0001"},
        },
    ]
    monkeypatch.setattr(module, "_iter_index_docs", lambda *args, **kwargs: iter(hits))

    data, doc_count = module.backup_user_index(client, "nde_user_profiles")

    assert client.indices.requests == ["nde_user_profiles"]
    assert doc_count == 2
    assert data["nde_user_profiles"]["aliases"] == {"users_alias": {}}
    assert data["nde_user_profiles"]["mappings"]["properties"]["username"]["type"] == "keyword"
    assert data["nde_user_profiles"]["docs"][0] == {
        "_id": "github:alice",
        "_source": {"username": "alice"},
    }
    assert data["nde_user_profiles"]["docs"][1]["_routing"] == "orcid"


def test_daily_backup_routine_writes_zip_and_uploads_to_s3(monkeypatch, tmp_path):
    module = _load_backup_module()
    client = FakeClient()
    s3_client = FakeS3Client()
    monkeypatch.setattr(
        module,
        "_iter_index_docs",
        lambda *args, **kwargs: iter(
            [{"_id": "github:alice", "_source": {"username": "alice"}}]
        ),
    )
    config = SimpleNamespace(
        ES_USER_INDEX="nde_user_profiles",
    )

    result = module.daily_backup_routine(
        config,
        client=client,
        s3_client=s3_client,
        local_dir=str(tmp_path),
        raise_on_error=True,
    )

    archive = tmp_path / result["archive"]
    assert archive.exists()
    assert result["s3_key"].startswith("es_backup/nde_user_profiles_backup_")
    assert s3_client.uploads == [
        {
            "filename": str(archive),
            "bucket": "nde",
            "key": result["s3_key"],
        }
    ]

    with zipfile.ZipFile(archive) as zfile:
        json_files = [name for name in zfile.namelist() if name.endswith(".json")]
        assert len(json_files) == 1
        payload = json.loads(zfile.read(json_files[0]))

    assert payload["nde_user_profiles"]["docs"] == [
        {"_id": "github:alice", "_source": {"username": "alice"}}
    ]


def test_daily_backup_routine_reports_user_index_and_doc_count(monkeypatch, tmp_path):
    module = _load_backup_module()
    client = FakeClient()
    s3_client = FakeS3Client()
    monkeypatch.setattr(
        module,
        "_iter_index_docs",
        lambda *args, **kwargs: iter(
            [{"_id": "github:alice", "_source": {"username": "alice"}}]
        ),
    )
    config = SimpleNamespace(
        ES_USER_INDEX="nde_user_profiles",
    )

    result = module.daily_backup_routine(
        config,
        client=client,
        s3_client=s3_client,
        local_dir=str(tmp_path),
        raise_on_error=True,
    )

    assert result["index"] == "nde_user_profiles"
    assert result["doc_count"] == 1


def test_read_backup_from_s3_uses_latest_backup():
    module = _load_backup_module()
    s3_client = FakeS3Client()
    old_payload = {"old": {"docs": []}}
    new_payload = {"nde_user_profiles": {"docs": [{"_id": "github:alice"}]}}
    old_key = "es_backup/nde_user_profiles_backup_20260701T000000Z.zip"
    new_key = "es_backup/nde_user_profiles_backup_20260702T000000Z.zip"
    s3_client.objects[("nde", old_key)] = _zip_payload("old.json", old_payload)
    s3_client.objects[("nde", new_key)] = _zip_payload("new.json", new_payload)
    s3_client.last_modified = {
        old_key: datetime(2026, 7, 1, tzinfo=timezone.utc),
        new_key: datetime(2026, 7, 2, tzinfo=timezone.utc),
    }

    key, payload = module.read_backup_from_s3(s3_client=s3_client)

    assert key == new_key
    assert payload == new_payload


def test_restore_from_s3_restores_latest_backup(monkeypatch):
    module = _load_backup_module()
    s3_client = FakeS3Client()
    backup_payload = {
        "nde_user_profiles": {
            "docs": [{"_id": "github:alice", "_source": {"username": "alice"}}]
        }
    }
    key = "es_backup/nde_user_profiles_backup_20260702T000000Z.zip"
    s3_client.objects[("nde", key)] = _zip_payload("backup.json", backup_payload)
    s3_client.last_modified = {key: datetime(2026, 7, 2, tzinfo=timezone.utc)}
    captured = {}

    def fake_restore_user_index(client, data, target_index, *, replace_index=False):
        captured["client"] = client
        captured["data"] = data
        captured["target_index"] = target_index
        captured["replace_index"] = replace_index
        return {
            "source_index": "nde_user_profiles",
            "target_index": target_index,
            "docs_restored": 1,
            "errors": 0,
        }

    monkeypatch.setattr(module, "restore_user_index", fake_restore_user_index)
    client = object()
    config = SimpleNamespace(ES_USER_INDEX="nde_user_profiles")

    result = module.restore_from_s3(config, client=client, s3_client=s3_client)

    assert captured["client"] is client
    assert captured["data"] == backup_payload
    assert captured["target_index"] == "nde_user_profiles"
    assert captured["replace_index"] is False
    assert result["s3_bucket"] == "nde"
    assert result["s3_key"] == key
    assert result["docs_restored"] == 1


def test_restore_user_index_bulk_indexes_docs(monkeypatch):
    module = _load_backup_module()

    class FakeRestoreIndices:
        def __init__(self):
            self.created = []

        def exists(self, *, index):
            return True

        def create(self, *, index, **body):
            self.created.append({"index": index, "body": body})

    client = SimpleNamespace(indices=FakeRestoreIndices())
    bulk_calls = []

    def fake_bulk(client_arg, actions, *, refresh, stats_only):
        bulk_calls.append(
            {
                "client": client_arg,
                "actions": list(actions),
                "refresh": refresh,
                "stats_only": stats_only,
            }
        )
        return len(actions), 0

    import elasticsearch.helpers

    monkeypatch.setattr(elasticsearch.helpers, "bulk", fake_bulk)
    backup_payload = {
        "nde_user_profiles": {
            "docs": [
                {"_id": "github:alice", "_source": {"username": "alice"}},
                {
                    "_id": "orcid:0000-0001",
                    "_routing": "orcid",
                    "_source": {"username": "0000-0001"},
                },
            ]
        }
    }

    result = module.restore_user_index(client, backup_payload, "nde_user_profiles")

    assert client.indices.created == []
    assert result["docs_restored"] == 2
    assert result["errors"] == 0
    assert bulk_calls[0]["refresh"] is True
    assert bulk_calls[0]["stats_only"] is True
    assert bulk_calls[0]["actions"][0] == {
        "_op_type": "index",
        "_index": "nde_user_profiles",
        "_id": "github:alice",
        "_source": {"username": "alice"},
    }
    assert bulk_calls[0]["actions"][1]["_routing"] == "orcid"


def test_restore_user_index_creates_missing_index_from_backup_metadata(monkeypatch):
    module = _load_backup_module()

    class FakeRestoreIndices:
        def __init__(self):
            self.deleted = []
            self.created = []
            self.exists_checks = []

        def exists(self, *, index):
            self.exists_checks.append(index)
            return index in self.deleted

        def delete(self, *, index):
            self.deleted.append(index)

        def create(self, *, index, **body):
            self.created.append({"index": index, "body": body})

    client = SimpleNamespace(indices=FakeRestoreIndices())
    monkeypatch.setattr("elasticsearch.helpers.bulk", lambda *args, **kwargs: (0, 0))
    backup_payload = {
        "nde_user_profiles": {
            "aliases": {"users_alias": {}},
            "mappings": {"properties": {"username": {"type": "keyword"}}},
            "settings": {
                "index": {
                    "creation_date": "1783458392285",
                    "number_of_shards": "1",
                    "provided_name": "nde_user_profiles",
                    "uuid": "original-index-uuid",
                    "version": {"created": "8503000"},
                }
            },
            "docs": [],
        }
    }

    result = module.restore_user_index(
        client,
        backup_payload,
        "nde_user_profiles_restore",
    )

    assert result["docs_restored"] == 0
    assert client.indices.created == [
        {
            "index": "nde_user_profiles_restore",
            "body": {
                "aliases": {"users_alias": {}},
                "mappings": {"properties": {"username": {"type": "keyword"}}},
                "settings": {"index": {"number_of_shards": "1"}},
            },
        }
    ]
