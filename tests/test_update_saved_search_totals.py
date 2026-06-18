import importlib.util
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "nde-web"
    / "scripts"
    / "update_saved_search_totals.py"
)


def _load_script_module():
    spec = importlib.util.spec_from_file_location(
        "update_saved_search_totals",
        SCRIPT_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeClient:
    def __init__(self, counts):
        self.counts = list(counts)
        self.updates = []
        self.indexes = []

    def count(self, *, index, query):
        return {"count": self.counts.pop(0)}

    def update(self, *, index, id, body):
        self.updates.append({"index": index, "id": id, "body": body})

    def index(self, *, index, id, body):
        self.indexes.append({"index": index, "id": id, "body": body})


def test_refresh_saved_search_totals_updates_changed_profile():
    module = _load_script_module()
    client = FakeClient([7, 2])
    hits = [
        {
            "_id": "github:alice",
            "_source": {
                "favorite_searches": [
                    {"name": "changed", "query": "covid", "filters": {}, "total": 3},
                    {"name": "same", "query": "__all__", "filters": {}, "total": 2},
                ]
            },
        }
    ]

    original_iter = module._iter_user_profiles
    module._iter_user_profiles = lambda *args, **kwargs: iter(hits)
    try:
        stats = module.refresh_saved_search_totals(
            client,
            user_index="users",
            data_index="data",
            batch_size=500,
            scroll="5m",
        )
    finally:
        module._iter_user_profiles = original_iter

    assert stats["profiles_seen"] == 1
    assert stats["profiles_changed"] == 1
    assert stats["saved_searches_changed"] == 1
    assert client.updates[0]["index"] == "users"
    assert client.updates[0]["id"] == "github:alice"
    assert client.updates[0]["body"]["doc"]["favorite_searches"][0]["total"] == 7


def test_extract_build_info_reads_metadata_release_fields():
    module = _load_script_module()

    build_info = module._extract_build_info(
        {
            "biothing_type": "dataset",
            "build_date": "2026-06-15T23:16:49.294558-07:00",
            "build_version": "20260615",
            "src": {},
        }
    )

    assert build_info == {
        "biothing_type": "dataset",
        "build_date": "2026-06-15T23:16:49.294558-07:00",
        "build_version": "20260615",
    }


def test_build_marker_matches_current_build_version():
    module = _load_script_module()

    assert module._build_marker_matches(
        {"build_version": "20260615", "build_date": "old"},
        {"build_version": "20260615"},
    )
    assert not module._build_marker_matches(
        {"build_version": "20260614"},
        {"build_version": "20260615"},
    )


def test_save_refresh_marker_records_build_info_and_stats():
    module = _load_script_module()
    client = FakeClient([])

    module._save_refresh_marker(
        client,
        user_index="users",
        build_info={"build_version": "20260615"},
        stats={"profiles_seen": 2},
    )

    assert client.indexes[0]["index"] == "users"
    assert client.indexes[0]["id"] == module.REFRESH_MARKER_ID
    assert client.indexes[0]["body"]["kind"] == "saved_search_totals_refresh"
    assert client.indexes[0]["body"]["build_version"] == "20260615"
    assert client.indexes[0]["body"]["stats"] == {"profiles_seen": 2}
