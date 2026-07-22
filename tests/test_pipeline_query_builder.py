import sys
import types
from pathlib import Path


WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))


config = types.ModuleType("config")
config.AI_SEARCH_VECTOR_FIELD = "embedding"
sys.modules["config"] = config

from pipeline import NDEQueryBuilder  # noqa: E402


def _find_query_string_clauses(value):
    if isinstance(value, dict):
        clauses = []
        if "query_string" in value:
            clauses.append(value["query_string"])
        for child in value.values():
            clauses.extend(_find_query_string_clauses(child))
        return clauses
    if isinstance(value, list):
        clauses = []
        for child in value:
            clauses.extend(_find_query_string_clauses(child))
        return clauses
    return []


def _contains_key(value, key):
    if isinstance(value, dict):
        return key in value or any(_contains_key(child, key) for child in value.values())
    if isinstance(value, list):
        return any(_contains_key(child, key) for child in value)
    return False


def test_size_zero_extra_filter_uses_filter_context_and_request_cache():
    body = NDEQueryBuilder().build(
        "__all__",
        extra_filter='(-_exists_:("infectiousAgent.displayName.raw"))',
        size=0,
        aggs=["infectiousAgent.displayName.raw"],
    ).to_dict()

    bool_query = body["query"]["bool"]

    assert body["request_cache"] is True
    assert {
        "query": '(-_exists_:("infectiousAgent.displayName.raw"))'
    } in _find_query_string_clauses(bool_query["filter"])
    assert not _find_query_string_clauses(bool_query.get("must", []))


def test_hit_returning_extra_filter_stays_in_query_context():
    body = NDEQueryBuilder().build(
        "__all__",
        extra_filter='(-_exists_:("infectiousAgent.displayName.raw"))',
        size=10,
    ).to_dict()

    bool_query = body["query"]["bool"]

    assert "request_cache" not in body
    assert {
        "query": '(-_exists_:("infectiousAgent.displayName.raw"))'
    } in _find_query_string_clauses(bool_query["must"])


def test_size_zero_requests_skip_function_score():
    body = NDEQueryBuilder().build(
        "__all__",
        extra_filter='(-_exists_:("infectiousAgent.displayName.raw"))',
        size=0,
        use_metadata_score="true",
    ).to_dict()

    assert not _contains_key(body, "function_score")
