import sys
from pathlib import Path


WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

from saved_search_counts import build_saved_search_count_body  # noqa: E402


def test_browse_query_uses_match_all_with_type_filter():
    body = build_saved_search_count_body("__all__", {})

    assert body["query"]["bool"]["must"] == [{"match_all": {}}]
    assert body["query"]["bool"]["filter"][0]["bool"]["minimum_should_match"] == 1


def test_simple_query_matches_public_search_shape():
    body = build_saved_search_count_body("covid data", {})
    queries = body["query"]["bool"]["must"][0]["dis_max"]["queries"]

    assert {"term": {"_id": {"value": "covid data", "boost": 5}}} in queries
    assert {
        "query_string": {
            "query": "covid* data*",
            "default_operator": "AND",
            "boost": 0.5,
            "lenient": True,
        }
    } in queries


def test_string_filter_is_query_string_clause():
    body = build_saved_search_count_body(
        "__all__",
        '(healthCondition.name:("asthma")) AND -_exists_:measurementTechnique.name',
    )

    assert body["query"]["bool"]["filter"][1] == {
        "query_string": {
            "query": '(healthCondition.name:("asthma")) AND -_exists_:measurementTechnique.name'
        }
    }


def test_mapping_filters_become_field_filters():
    body = build_saved_search_count_body(
        "covid",
        {"includedInDataCatalog.name": ["Zenodo", "Figshare"], "@type": "Dataset"},
    )

    assert {"terms": {"includedInDataCatalog.name": ["Zenodo", "Figshare"]}} in (
        body["query"]["bool"]["filter"]
    )
    assert {"term": {"@type": "Dataset"}} in body["query"]["bool"]["filter"]


def test_elasticsearch_query_filter_is_preserved():
    es_filter = {"range": {"date": {"gte": "2020-01-01", "lte": "2020-12-31"}}}
    body = build_saved_search_count_body("__all__", es_filter)

    assert es_filter in body["query"]["bool"]["filter"]
