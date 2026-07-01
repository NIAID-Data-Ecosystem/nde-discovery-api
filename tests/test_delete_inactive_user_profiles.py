import importlib.util
import sys
import types
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "nde-web"
    / "scripts"
    / "delete_inactive_user_profiles.py"
)


def _load_script_module():
    spec = importlib.util.spec_from_file_location(
        "delete_inactive_user_profiles",
        SCRIPT_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeClient:
    def __init__(self):
        self.deletes = []

    def delete(self, *, index, id):
        self.deletes.append({"index": index, "id": id})


def test_delete_inactive_user_profiles_deletes_only_inactive_profiles():
    module = _load_script_module()
    client = FakeClient()
    hits = [
        {
            "_id": "github:old",
            "_source": {"last_active": "2024-06-30T23:59:59+00:00"},
        },
        {
            "_id": "orcid:legacy-old",
            "_source": {"updated": "2024-01-01T00:00:00+00:00"},
        },
        {
            "_id": "github:active",
            "_source": {"last_active": "2024-07-01T00:00:00+00:00"},
        },
        {
            "_id": "_saved_search_totals_refresh",
            "_source": {"kind": "saved_search_totals_refresh"},
        },
        {
            "_id": "github:unknown",
            "_source": {"username": "unknown"},
        },
    ]

    original_iter = module._iter_user_profiles
    module._iter_user_profiles = lambda *args, **kwargs: iter(hits)
    try:
        stats = module.delete_inactive_user_profiles(
            client,
            user_index="users",
            cutoff="2024-07-01T00:00:00+00:00",
            batch_size=500,
            scroll="5m",
        )
    finally:
        module._iter_user_profiles = original_iter

    assert [delete["id"] for delete in client.deletes] == [
        "github:old",
        "orcid:legacy-old",
    ]
    assert stats["profiles_seen"] == 4
    assert stats["profiles_deleted"] == 2
    assert stats["profiles_active"] == 1
    assert stats["profiles_without_activity"] == 1
    assert stats["system_docs_skipped"] == 1


def test_delete_inactive_user_profiles_dry_run_does_not_delete():
    module = _load_script_module()
    client = FakeClient()
    hits = [
        {
            "_id": "github:old",
            "_source": {"last_active": "2024-06-30T23:59:59+00:00"},
        },
    ]

    original_iter = module._iter_user_profiles
    module._iter_user_profiles = lambda *args, **kwargs: iter(hits)
    try:
        stats = module.delete_inactive_user_profiles(
            client,
            user_index="users",
            cutoff="2024-07-01T00:00:00+00:00",
            batch_size=500,
            scroll="5m",
            dry_run=True,
        )
    finally:
        module._iter_user_profiles = original_iter

    assert client.deletes == []
    assert stats["profiles_would_delete"] == 1
    assert stats["profiles_deleted"] == 0


def test_resolve_cutoff_defaults_to_two_calendar_years():
    module = _load_script_module()

    cutoff = module._resolve_cutoff(
        inactive_years=2,
        now=datetime(2026, 7, 1, tzinfo=timezone.utc),
    )

    assert cutoff == datetime(2024, 7, 1, tzinfo=timezone.utc)


def test_apply_config_defaults_reads_user_index_from_config_module():
    module = _load_script_module()
    config_name = "fake_nde_config_for_inactive_user_cleanup"
    config = types.ModuleType(config_name)
    config.ES_HOST = "http://172.30.2.11:9200"
    config.ES_USER_INDEX = "custom_user_profiles"
    config.ES_ARGS = {
        "request_timeout": 120,
        "max_retries": 5,
        "http_compress": True,
    }
    sys.modules[config_name] = config

    args = types.SimpleNamespace(
        config_module=config_name,
        es_host=None,
        user_index=None,
        request_timeout=None,
    )
    try:
        args = module._apply_config_defaults(args)
    finally:
        sys.modules.pop(config_name, None)

    assert args.es_host == "http://172.30.2.11:9200"
    assert args.user_index == "custom_user_profiles"
    assert args.request_timeout == 120
    assert args.es_args["max_retries"] == 5
    assert args.es_args["http_compress"] is True
