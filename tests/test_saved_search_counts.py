import sys
from pathlib import Path


WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

from saved_search_counts import (  # noqa: E402
    build_saved_search_count_body,
    build_saved_search_extra_filter,
    frontend_default_extra_filter,
)


def test_browse_query_uses_match_all_with_type_filter():
    body = build_saved_search_count_body(
        "__all__",
        {},
        include_frontend_defaults=False,
        include_exclusions=False,
    )

    assert body["query"]["bool"]["must"] == [{"match_all": {}}]
    assert body["query"]["bool"]["filter"][0]["bool"]["minimum_should_match"] == 1


def test_count_body_uses_main_frontend_type_filter():
    body = build_saved_search_count_body(
        "__all__",
        {},
        include_frontend_defaults=False,
        include_exclusions=False,
    )
    type_filter = body["query"]["bool"]["filter"][0]["bool"]

    assert type_filter["should"][0] == {
        "terms": {"@type": ["Dataset", "ResourceCatalog"]}
    }
    assert "Sample" not in type_filter["should"][0]["terms"]["@type"]
    assert "DataCollection" not in type_filter["should"][0]["terms"]["@type"]
    assert type_filter["should"][1] == {
        "bool": {
            "must": [
                {"term": {"@type": "ComputationalTool"}},
                {"term": {"includedInDataCatalog.name": "bio.tools"}},
            ]
        }
    }
    assert type_filter["should"][2] == {
        "bool": {
            "must": [
                {"term": {"@type": "Sample"}},
                {"term": {"additionalType": "BioSample"}},
                {"term": {"includedInDataCatalog.name": "BEI Resources"}},
            ]
        }
    }
    assert len(type_filter["should"]) == 3
    assert "ExperimentalRunSample" not in str(type_filter)


def test_count_body_applies_main_exclusions():
    body = build_saved_search_count_body(
        "__all__",
        {},
        include_frontend_defaults=False,
        exclusions={
            "prod_catalogs": ["Zenodo", "NCBI SRA"],
            "staging_ids": ["staging-doc"],
        },
    )

    assert body["query"]["bool"]["filter"][1] == {
        "terms": {"includedInDataCatalog.name": ["Zenodo", "NCBI SRA"]}
    }
    assert body["query"]["bool"]["filter"][2] == {
        "bool": {"must_not": [{"ids": {"values": ["staging-doc"]}}]}
    }


def test_simple_query_matches_public_search_shape():
    body = build_saved_search_count_body(
        "covid data",
        {},
        include_frontend_defaults=False,
        include_exclusions=False,
    )
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
        include_frontend_defaults=False,
        include_exclusions=False,
    )

    assert body["query"]["bool"]["filter"][1] == {
        "query_string": {
            "query": '((healthCondition.name:("asthma")) AND -_exists_:measurementTechnique.name)'
        }
    }


def test_mapping_filters_become_field_filters():
    body = build_saved_search_count_body(
        "covid",
        {"includedInDataCatalog.name": ["Zenodo", "Figshare"], "@type": "Dataset"},
        include_frontend_defaults=False,
        include_exclusions=False,
    )

    assert body["query"]["bool"]["filter"][1] == {
        "query_string": {
            "query": '(includedInDataCatalog.name:("Zenodo" OR "Figshare") AND @type:"Dataset")'
        }
    }


def test_frontend_date_filter_with_missing_date_uses_or_range():
    extra_filter = build_saved_search_extra_filter(
        {"date": ["2000-01-01", "2026-12-31"], "-_exists_": ["date"]},
        include_frontend_defaults=False,
    )

    assert extra_filter == '((date:["2000-01-01" TO "2026-12-31"] OR (-_exists_:("date"))))'


def test_frontend_date_filter_with_exists_keeps_date_or_grouped():
    extra_filter = build_saved_search_extra_filter(
        {
            "date": ["2000-01-01", "2026-12-31"],
            "_exists_": ["species.displayName.raw"],
            "-_exists_": ["date"],
        },
        include_frontend_defaults=False,
    )

    assert extra_filter == (
        '((date:["2000-01-01" TO "2026-12-31"] OR (-_exists_:("date"))) '
        'AND _exists_:("species.displayName.raw"))'
    )


def test_positive_and_negative_exists_filters_are_grouped_by_field():
    extra_filter = build_saved_search_extra_filter(
        {
            "_exists_": ["species.displayName.raw", "includedInDataCatalog.name"],
            "-_exists_": ["species.displayName.raw", "measurementTechnique.name.raw"],
        },
        include_frontend_defaults=False,
    )

    assert extra_filter == (
        '(_exists_:("includedInDataCatalog.name") '
        'AND -_exists_:("measurementTechnique.name.raw") '
        'AND ((_exists_:("species.displayName.raw")) OR (-_exists_:("species.displayName.raw"))))'
    )


def test_mixed_value_and_exists_filters_keep_frontend_or_semantics():
    extra_filter = build_saved_search_extra_filter(
        {
            "species.displayName.raw": ["Human | Homo sapiens"],
            "_exists_": ["species.displayName.raw"],
            "-_exists_": ["measurementTechnique.name.raw"],
        },
        include_frontend_defaults=False,
    )

    assert extra_filter == (
        '((species.displayName.raw:("Human | Homo sapiens") '
        'OR (_exists_:("species.displayName.raw"))) '
        'AND -_exists_:("measurementTechnique.name.raw"))'
    )


def test_mixed_value_and_missing_filters_keep_frontend_or_semantics():
    extra_filter = build_saved_search_extra_filter(
        {
            "measurementTechnique.name.raw": ["ELISA"],
            "-_exists_": ["measurementTechnique.name.raw"],
        },
        include_frontend_defaults=False,
    )

    assert extra_filter == (
        '((measurementTechnique.name.raw:("ELISA") '
        'OR (-_exists_:("measurementTechnique.name.raw"))))'
    )


def test_elasticsearch_query_filter_is_preserved():
    es_filter = {"range": {"date": {"gte": "2020-01-01", "lte": "2020-12-31"}}}
    body = build_saved_search_count_body(
        "__all__",
        es_filter,
        include_frontend_defaults=False,
        include_exclusions=False,
    )

    assert body["query"]["bool"]["filter"][1] == {
        "query_string": {"query": '(date:["2020-01-01" TO "2020-12-31"])'}
    }


def test_frontend_default_extra_filter_matches_visible_records():
    extra_filter = frontend_default_extra_filter()

    assert extra_filter == 'NOT(@type:Sample AND NOT additionalType:"BioSample")'


def test_saved_search_extra_filter_keeps_empty_filters_date_free():
    extra_filter = build_saved_search_extra_filter({})

    assert extra_filter == '(NOT(@type:Sample AND NOT additionalType:"BioSample"))'


def test_saved_search_extra_filter_combines_frontend_visibility_and_user_filters():
    extra_filter = build_saved_search_extra_filter(
        {"healthCondition.name": ["asthma", "diabetes"]},
    )

    assert (
        extra_filter
        == '(NOT(@type:Sample AND NOT additionalType:"BioSample")) '
        'AND (healthCondition.name:("asthma" OR "diabetes"))'
    )


def test_explicit_date_filter_is_preserved_with_frontend_visibility():
    extra_filter = build_saved_search_extra_filter(
        {"date": ["1990-01-01", "1999-12-31"]},
    )

    assert extra_filter == (
        '(NOT(@type:Sample AND NOT additionalType:"BioSample")) '
        'AND (date:["1990-01-01" TO "1999-12-31"])'
    )


def test_explicit_date_exists_filter_skips_date_default():
    extra_filter = build_saved_search_extra_filter(
        {"_exists_": ["date"]},
    )

    assert extra_filter == (
        '(NOT(@type:Sample AND NOT additionalType:"BioSample")) '
        'AND (_exists_:("date"))'
    )
