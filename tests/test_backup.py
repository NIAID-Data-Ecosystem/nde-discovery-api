import importlib.util
import json
import sys
import zipfile
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

    def upload_file(self, filename, bucket, key):
        self.uploads.append({"filename": filename, "bucket": bucket, "key": key})


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

    archive = Path(result["archive"])
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
