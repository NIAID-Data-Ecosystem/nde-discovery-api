"""Build count queries for user saved searches.

The user profile index stores saved searches as plain JSON objects.  These
helpers keep the stored ``total`` calculation aligned with the public search
query shape without depending on Tornado or BioThings runtime objects.
"""

import json
from collections.abc import Iterable, Mapping
from pathlib import Path


DEFAULT_DATA_INDEX = "nde_all_current"
DEFAULT_USER_INDEX = "nde_user_profiles"

_BROWSE_ALL_QUERIES = frozenset({"", "__all__", "__any__", "*", "*:*"})
_SUPPORTED_TYPES = ["Dataset", "ResourceCatalog"]
_DEFAULT_SAMPLE_VISIBILITY_FILTER = (
    'NOT(@type:Sample AND NOT additionalType:"BioSample")'
)
_EXCLUSIONS_PATH = Path(__file__).with_name("exclusions.json")


def build_saved_search_count_body(
    query: str | None,
    filters=None,
    *,
    include_frontend_defaults: bool = True,
    include_exclusions: bool = True,
    exclusions: Mapping | None = None,
) -> dict:
    """Return an Elasticsearch count body for a saved search entry."""
    extra_filter = build_saved_search_extra_filter(
        filters,
        include_frontend_defaults=include_frontend_defaults,
    )
    bool_query = {
        "must": [_build_query_clause(query)],
        "filter": [
            _build_type_filter(),
            *_build_exclusion_filter_clauses(
                exclusions=exclusions,
                include_exclusions=include_exclusions,
            ),
            *_build_filter_clauses(extra_filter),
        ],
    }
    return {"query": {"bool": bool_query}}


def build_saved_search_extra_filter(
    filters=None,
    *,
    include_frontend_defaults: bool = True,
) -> str | None:
    """Return the frontend-equivalent extra_filter for a saved search."""
    clauses = []
    user_filter = _filters_to_query_string(filters)

    if include_frontend_defaults:
        if not _looks_like_frontend_default_filter(user_filter):
            clauses.append(frontend_default_extra_filter())

    if user_filter:
        clauses.append(user_filter)

    return " AND ".join(f"({clause})" for clause in clauses) or None


def frontend_default_extra_filter() -> str:
    """Default frontend visibility filter applied to ordinary search totals."""
    return _DEFAULT_SAMPLE_VISIBILITY_FILTER


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


def _load_exclusions() -> dict:
    try:
        with _EXCLUSIONS_PATH.open() as handle:
            return json.load(handle)
    except FileNotFoundError:
        return {}


def _build_exclusion_filter_clauses(
    *,
    exclusions: Mapping | None = None,
    include_exclusions: bool = True,
) -> list[dict]:
    if not include_exclusions:
        return []

    exclusions = exclusions if exclusions is not None else _load_exclusions()
    if not isinstance(exclusions, Mapping):
        return []

    clauses = []
    prod_catalogs = _filter_values(exclusions.get("prod_catalogs"))
    if prod_catalogs:
        clauses.append({"terms": {"includedInDataCatalog.name": prod_catalogs}})

    staging_ids = _filter_values(exclusions.get("staging_ids"))
    if staging_ids:
        clauses.append({"bool": {"must_not": [{"ids": {"values": staging_ids}}]}})

    return clauses


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


def _build_filter_clauses(extra_filter: str | None) -> list[dict]:
    if not extra_filter:
        return []

    return [{"query_string": {"query": extra_filter}}]


def _filters_to_query_string(filters) -> str | None:
    if filters in (None, "", False):
        return None

    if isinstance(filters, str):
        return filters.strip() or None

    if isinstance(filters, Mapping):
        return _mapping_filter_to_query_string(filters)

    if isinstance(filters, Iterable):
        clauses = [_filters_to_query_string(item) for item in filters]
        clauses = [clause for clause in clauses if clause]
        return " AND ".join(f"({clause})" for clause in clauses) or None

    return None


def _mapping_filter_to_query_string(filters: Mapping) -> str | None:
    if not filters:
        return None

    if "query_string" in filters:
        query_string = filters.get("query_string") or {}
        if isinstance(query_string, Mapping):
            return query_string.get("query")

    if "extra_filter" in filters:
        return _filters_to_query_string(filters["extra_filter"])
    if "filter" in filters:
        return _filters_to_query_string(filters["filter"])
    if "filters" in filters:
        return _filters_to_query_string(filters["filters"])

    es_clause = _es_filter_clause_to_query_string(filters)
    if es_clause:
        return es_clause

    exists_fields = set(str(field) for field in _filter_values(filters.get("_exists_")))
    missing_fields = set(str(field) for field in _filter_values(filters.get("-_exists_")))
    exists_fields_used = set()
    missing_fields_used = set()
    clauses = []
    for field, value in filters.items():
        if field in {"_exists_", "-_exists_"}:
            continue

        clause = _field_filter_to_query_string(field, value)
        if clause:
            field_clauses = [clause]
            if field in exists_fields:
                field_clauses.append(f"({_exists_field_filter_to_query_string(field)})")
                exists_fields_used.add(field)
            if field in missing_fields:
                field_clauses.append(f"({_missing_field_filter_to_query_string(field)})")
                missing_fields_used.add(field)
            if len(field_clauses) > 1:
                clause = f"({' OR '.join(field_clauses)})"
            clauses.append(clause)

    for field in sorted((exists_fields | missing_fields) - (exists_fields_used | missing_fields_used)):
        field_clauses = []
        if field in exists_fields:
            field_clauses.append(_exists_field_filter_to_query_string(field))
        if field in missing_fields:
            field_clauses.append(_missing_field_filter_to_query_string(field))
        if len(field_clauses) == 1:
            clauses.append(field_clauses[0])
        elif field_clauses:
            clauses.append(f"({' OR '.join(f'({clause})' for clause in field_clauses)})")

    return " AND ".join(clauses) or None


def _es_filter_clause_to_query_string(filters: Mapping) -> str | None:
    if set(filters) == {"term"}:
        term = filters["term"]
        if isinstance(term, Mapping) and len(term) == 1:
            field, value = next(iter(term.items()))
            if isinstance(value, Mapping) and "value" in value:
                value = value["value"]
            return _field_filter_to_query_string(field, value)

    if set(filters) == {"terms"}:
        terms = filters["terms"]
        if isinstance(terms, Mapping) and len(terms) == 1:
            field, values = next(iter(terms.items()))
            return _field_filter_to_query_string(field, values)

    if set(filters) == {"exists"}:
        exists = filters["exists"]
        if isinstance(exists, Mapping) and exists.get("field"):
            return f'_exists_:{exists["field"]}'

    if set(filters) == {"range"}:
        ranges = filters["range"]
        if isinstance(ranges, Mapping) and len(ranges) == 1:
            field, bounds = next(iter(ranges.items()))
            if isinstance(bounds, Mapping):
                lower = bounds.get("gte", bounds.get("gt", "*"))
                upper = bounds.get("lte", bounds.get("lt", "*"))
                return (
                    f"{field}:[{_quote_query_value(lower)} "
                    f"TO {_quote_query_value(upper)}]"
                )

    return None


def _field_filter_to_query_string(field: str, value) -> str | None:
    if value in (None, "", False):
        return None

    date_range_clause = _date_range_filter_to_query_string(field, value)
    if date_range_clause:
        return date_range_clause

    if isinstance(value, Mapping):
        if "values" in value:
            return _field_filter_to_query_string(field, value["values"])
        if "value" in value:
            return _field_filter_to_query_string(field, value["value"])
        return None

    if isinstance(value, str):
        return f"{field}:{_quote_query_value(value)}"

    if isinstance(value, Iterable):
        values = [item for item in value if item not in (None, "", False)]
        if not values:
            return None
        return f"{field}:({' OR '.join(_quote_query_value(item) for item in values)})"

    return f"{field}:{_quote_query_value(value)}"


def _date_range_filter_to_query_string(field: str, value) -> str | None:
    if field != "date":
        return None

    values = _filter_values(value)
    if len(values) != 2:
        return None

    lower, upper = values
    return f"{field}:[{_quote_query_value(lower)} TO {_quote_query_value(upper)}]"


def _missing_field_filter_to_query_string(field: str) -> str | None:
    if field in (None, "", False):
        return None
    return f"-_exists_:({_quote_query_value(field)})"


def _exists_field_filter_to_query_string(field: str) -> str | None:
    if field in (None, "", False):
        return None
    return f"_exists_:({_quote_query_value(field)})"


def _filter_values(value) -> list:
    if value in (None, "", False):
        return []

    if isinstance(value, Mapping):
        if "values" in value:
            return _filter_values(value["values"])
        if "value" in value:
            return _filter_values(value["value"])
        return []

    if isinstance(value, str):
        return [value]

    if isinstance(value, Iterable):
        return [item for item in value if item not in (None, "", False)]

    return [value]


def _quote_query_value(value) -> str:
    if value == "*":
        return "*"
    text = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def _looks_like_frontend_default_filter(extra_filter: str | None) -> bool:
    if not extra_filter:
        return False
    return (
        "NOT(@type:Sample AND NOT additionalType" in extra_filter
        or "NOT+(@type:Sample+AND+NOT+additionalType" in extra_filter
    )
