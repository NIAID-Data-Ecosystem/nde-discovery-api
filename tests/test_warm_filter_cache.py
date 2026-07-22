import importlib.util
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "nde-web"
    / "scripts"
    / "warm_filter_cache.py"
)


def _load_script_module():
    spec = importlib.util.spec_from_file_location("warm_filter_cache", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeUrlopenResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def test_query_url_is_derived_from_metadata_url():
    module = _load_script_module()

    assert (
        module._query_url_from_metadata_url(
            "https://api-staging.data.niaid.nih.gov/v1/metadata"
        )
        == "https://api-staging.data.niaid.nih.gov/v1/query"
    )


def test_build_warm_queries_generates_legacy_specified_and_unspecified_filters():
    module = _load_script_module()

    queries = list(
        module.build_warm_queries(
            q="__all__",
            fields=["infectiousAgent.displayName.raw"],
            scopes=["unscoped"],
            date_mode="none",
            exists_syntax="legacy",
            date_end_year=2026,
        )
    )

    assert [query.name for query in queries] == [
        "unscoped:no-date:base",
        "unscoped:no-date:legacy:infectiousAgent.displayName.raw:specified",
        "unscoped:no-date:legacy:infectiousAgent.displayName.raw:unspecified",
    ]
    assert "extra_filter" not in queries[0].params
    assert queries[1].params["extra_filter"] == (
        '(infectiousAgent.displayName.raw:'
        '(_exists_:("infectiousAgent.displayName.raw")))'
    )
    assert queries[2].params["extra_filter"] == (
        '(infectiousAgent.displayName.raw:'
        '(-_exists_:("infectiousAgent.displayName.raw")))'
    )
    assert queries[2].params["size"] == 0
    assert queries[2].params["facet_size"] == 1000
    assert queries[2].params["hist"] == "date"


def test_build_warm_queries_can_generate_canonical_default_date_scope_filter():
    module = _load_script_module()

    query = list(
        module.build_warm_queries(
            q="__all__",
            fields=["species.displayName.raw"],
            scopes=["shared_dataset"],
            date_mode="default",
            exists_syntax="canonical",
            facet_mode="category",
            date_end_year=2026,
            include_base=False,
        )
    )[1]

    assert query.name == (
        "shared_dataset:default-date:canonical:"
        "species.displayName.raw:unspecified"
    )
    assert query.params["facets"] == ",".join(module.SHARED_DATASET_FIELDS)
    assert query.params["extra_filter"] == (
        '(date:["2000-01-01" TO "2026-12-31"] OR (-_exists_:("date"))) '
        'AND (-_exists_:("species.displayName.raw")) '
        'AND NOT (@type:Sample AND NOT additionalType:"BioSample")'
    )


def test_warm_filter_cache_calls_query_api_with_encoded_params():
    module = _load_script_module()
    opened_urls = []

    def fake_urlopen(request, timeout):
        opened_urls.append((request.full_url, timeout))
        return FakeUrlopenResponse({"total": 123})

    original_urlopen = module.urlopen
    module.urlopen = fake_urlopen
    try:
        stats = module.warm_filter_cache(
            "https://api-staging.data.niaid.nih.gov/v1/query",
            [
                module.WarmQuery(
                    name="one",
                    params={
                        "q": "__all__",
                        "size": 0,
                        "facet_size": 1000,
                        "facets": "date",
                        "hist": "date",
                        "extra_filter": '(-_exists_:("date"))',
                    },
                )
            ],
            timeout=25,
        )
    finally:
        module.urlopen = original_urlopen

    assert stats == {"queued": 1, "completed": 1, "failed": 0}
    assert opened_urls[0][1] == 25

    parsed = urlsplit(opened_urls[0][0])
    params = parse_qs(parsed.query)
    assert params["q"] == ["__all__"]
    assert params["size"] == ["0"]
    assert params["facet_size"] == ["1000"]
    assert params["facets"] == ["date"]
    assert params["hist"] == ["date"]
    assert params["extra_filter"] == ['(-_exists_:("date"))']
