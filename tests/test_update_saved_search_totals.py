import importlib.util
import json
import sys
import types
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


class FakeUrlopenResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


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


def test_refresh_saved_search_totals_can_count_with_query_api():
    module = _load_script_module()
    client = FakeClient([])
    hits = [
        {
            "_id": "github:alice",
            "_source": {
                "favorite_searches": [
                    {
                        "name": "asthma",
                        "query": "asthma",
                        "filters": {},
                        "total": 99999,
                    },
                ]
            },
        }
    ]

    opened_urls = []

    def fake_urlopen(request, timeout):
        opened_urls.append(request.full_url)
        return FakeUrlopenResponse({"total": 11500})

    original_iter = module._iter_user_profiles
    original_urlopen = module.urlopen
    module._iter_user_profiles = lambda *args, **kwargs: iter(hits)
    module.urlopen = fake_urlopen
    try:
        stats = module.refresh_saved_search_totals(
            client,
            user_index="users",
            data_index="data",
            batch_size=500,
            scroll="5m",
            query_url="https://api-staging.data.niaid.nih.gov/v1/query",
        )
    finally:
        module._iter_user_profiles = original_iter
        module.urlopen = original_urlopen

    assert stats["saved_searches_changed"] == 1
    assert client.updates[0]["body"]["doc"]["favorite_searches"][0]["total"] == 11500
    assert opened_urls == [
        "https://api-staging.data.niaid.nih.gov/v1/query?q=asthma&size=0&facet_size=0"
    ]


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


def test_query_url_is_derived_from_metadata_url():
    module = _load_script_module()

    assert (
        module._query_url_from_metadata_url(
            "https://api-staging.data.niaid.nih.gov/v1/metadata"
        )
        == "https://api-staging.data.niaid.nih.gov/v1/query"
    )


def test_saved_search_api_params_convert_filters_to_extra_filter():
    module = _load_script_module()

    params = module._saved_search_api_params(
        {
            "query": "__all__",
            "filters": {
                "healthCondition.name": ["asthma", "diabetes"],
                "@type": "Dataset",
            },
        }
    )

    assert params == {
        "q": "__all__",
        "size": 0,
        "facet_size": 0,
        "extra_filter": 'healthCondition.name:("asthma" OR "diabetes") AND @type:"Dataset"',
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


def test_apply_config_defaults_reads_es_settings_from_config_module():
    module = _load_script_module()
    config_name = "fake_nde_config_for_saved_search_totals"
    config = types.ModuleType(config_name)
    config.ES_HOST = "http://172.30.2.11:9200"
    config.ES_USER_INDEX = "custom_user_profiles"
    config.ES_INDICES = {None: "custom_data_current"}
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
        data_index=None,
        metadata_url=None,
        query_url=None,
        request_timeout=None,
    )
    try:
        args = module._apply_config_defaults(args)
    finally:
        sys.modules.pop(config_name, None)

    assert args.es_host == "http://172.30.2.11:9200"
    assert args.user_index == "custom_user_profiles"
    assert args.data_index == "custom_data_current"
    assert args.request_timeout == 120
    assert args.es_args["max_retries"] == 5
    assert args.es_args["http_compress"] is True
