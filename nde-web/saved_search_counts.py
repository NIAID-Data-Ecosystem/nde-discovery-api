"""Build count queries for user saved searches.

The user profile index stores saved searches as plain JSON objects.  These
helpers keep the stored ``total`` calculation aligned with the public search
query shape without depending on Tornado or BioThings runtime objects.
"""

from collections.abc import Iterable, Mapping


DEFAULT_DATA_INDEX = "nde_all_current"
DEFAULT_USER_INDEX = "nde_user_profiles"

_BROWSE_ALL_QUERIES = frozenset({"", "__all__", "__any__", "*", "*:*"})
_SUPPORTED_TYPES = ["Dataset", "ResourceCatalog", "Sample", "DataCollection"]

_ES_QUERY_KEYS = frozenset(
    {
        "bool",
        "boosting",
        "constant_score",
        "dis_max",
        "exists",
        "function_score",
        "fuzzy",
        "ids",
        "match",
        "match_all",
        "match_none",
        "multi_match",
        "nested",
        "prefix",
        "query_string",
        "range",
        "regexp",
        "simple_query_string",
        "term",
        "terms",
        "wildcard",
    }
)


def build_saved_search_count_body(query: str | None, filters=None) -> dict:
    """Return an Elasticsearch count body for a saved search entry."""
    bool_query = {
        "must": [_build_query_clause(query)],
        "filter": [_build_type_filter(), *_build_filter_clauses(filters)],
    }
    return {"query": {"bool": bool_query}}


def _build_type_filter() -> dict:
    computational_tool_condition = {
        "bool": {
            "must": [
                {"term": {"@type": "ComputationalTool"}},
                {"term": {"includedInDataCatalog.name": "bio.tools"}},
            ]
        }
    }
    return {
        "bool": {
            "should": [
                {"terms": {"@type": _SUPPORTED_TYPES}},
                computational_tool_condition,
            ],
            "minimum_should_match": 1,
        }
    }


def _build_query_clause(query: str | None) -> dict:
    q = str(query or "").strip()
    if q in _BROWSE_ALL_QUERIES:
        return {"match_all": {}}

    # Mirrors pipeline.NDEQueryBuilder.default_string_query, minus scoring
    # wrappers that do not affect count results.
    if ":" in q or " AND " in q or " OR " in q:
        return {
            "query_string": {
                "query": q,
                "default_operator": "AND",
                "lenient": True,
            }
        }

    if q.startswith('"') and q.endswith('"'):
        unquoted = q.strip('"')
        return {
            "dis_max": {
                "queries": [
                    {"term": {"_id": {"value": unquoted, "boost": 5}}},
                    {"term": {"name": {"value": unquoted, "boost": 5}}},
                    {
                        "query_string": {
                            "query": q,
                            "default_operator": "AND",
                            "lenient": True,
                        }
                    },
                ]
            }
        }

    queries = [
        {"term": {"_id": {"value": q, "boost": 5}}},
        {"term": {"name": {"value": q, "boost": 5}}},
        {
            "query_string": {
                "query": q,
                "default_operator": "AND",
                "lenient": True,
            }
        },
    ]
    if "*" not in q and "?" not in q:
        queries.append(
            {
                "query_string": {
                    "query": "* ".join(q.split()) + "*",
                    "default_operator": "AND",
                    "boost": 0.5,
                    "lenient": True,
                }
            }
        )
    return {"dis_max": {"queries": queries}}


def _build_filter_clauses(filters) -> list[dict]:
    if filters in (None, "", False):
        return []

    if isinstance(filters, str):
        stripped = filters.strip()
        return [{"query_string": {"query": stripped}}] if stripped else []

    if isinstance(filters, Mapping):
        return _build_mapping_filter_clauses(filters)

    if isinstance(filters, Iterable):
        clauses = []
        for item in filters:
            clauses.extend(_build_filter_clauses(item))
        return clauses

    return []


def _build_mapping_filter_clauses(filters: Mapping) -> list[dict]:
    if not filters:
        return []

    if _is_es_query_clause(filters):
        return [dict(filters)]

    clauses = []
    for field, value in filters.items():
        if field == "filters":
            clauses.extend(_build_filter_clauses(value))
            continue
        if field in {"extra_filter", "filter"}:
            clauses.extend(_build_filter_clauses(value))
            continue
        clauses.extend(_field_filter_clause(field, value))
    return clauses


def _field_filter_clause(field: str, value) -> list[dict]:
    if value in (None, "", False):
        return []

    if isinstance(value, Mapping):
        if _is_es_query_clause(value):
            return [dict(value)]
        if "values" in value:
            return _field_filter_clause(field, value["values"])
        if "value" in value:
            return _field_filter_clause(field, value["value"])
        return [
            {"term": {f"{field}.{subfield}": subvalue}}
            for subfield, subvalue in value.items()
            if subvalue not in (None, "", False)
        ]

    if isinstance(value, str):
        return [{"term": {field: value}}]

    if isinstance(value, Iterable):
        values = [item for item in value if item not in (None, "", False)]
        if not values:
            return []
        if len(values) == 1:
            return [{"term": {field: values[0]}}]
        return [{"terms": {field: values}}]

    return [{"term": {field: value}}]


def _is_es_query_clause(value: Mapping) -> bool:
    return len(value) == 1 and next(iter(value)) in _ES_QUERY_KEYS
