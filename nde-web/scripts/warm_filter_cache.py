#!/usr/bin/env python3
"""Warm query API caches for common filter-sidebar aggregation requests.

Run this after publishing a new search index, moving an alias, clearing ES
caches, or restarting Elasticsearch nodes. The script calls the public query
API so Elasticsearch sees the same request bodies the portal generates.
"""

import argparse
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen


logger = logging.getLogger(__name__)

FILTER_FIELDS = (
    "date",
    "includedInDataCatalog.name",
    "sourceOrganization.name.raw",
    "healthCondition.name.raw",
    "infectiousAgent.displayName.raw",
    "species.displayName.raw",
    "funding.funder.name.raw",
    "conditionsOfAccess",
    "variableMeasured.name.raw",
    "measurementTechnique.name.raw",
    "topicCategory.name.raw",
    "applicationCategory.raw",
    "operatingSystem.raw",
    "programmingLanguage.raw",
    "featureList.name.raw",
    "input.name.raw",
    "output.name.raw",
    "anatomicalSystem.name",
    "associatedGenotype",
    "associatedPhenotype.name",
    "cellType.name",
    "instrument.name",
    "sampleType.name",
    "sex",
    "about.name",
    "exampleOfWork.about.name.raw",
)

SHARED_DATASET_FIELDS = FILTER_FIELDS[:11]
COMPUTATIONAL_TOOL_FIELDS = FILTER_FIELDS[11:17]
SAMPLE_FIELDS = FILTER_FIELDS[17:24]
DATA_COLLECTION_FIELDS = FILTER_FIELDS[24:]

SCOPES = {
    "unscoped": {"extra_filter": "", "fields": FILTER_FIELDS},
    "shared_dataset": {
        "extra_filter": 'NOT (@type:Sample AND NOT additionalType:"BioSample")',
        "fields": SHARED_DATASET_FIELDS,
    },
    "biosample": {
        "extra_filter": '@type:Sample AND additionalType:"BioSample"',
        "fields": SAMPLE_FIELDS,
    },
    "computational_tool": {
        "extra_filter": "@type:ComputationalTool",
        "fields": COMPUTATIONAL_TOOL_FIELDS,
    },
    "data_collection": {
        "extra_filter": "@type:DataCollection",
        "fields": DATA_COLLECTION_FIELDS,
    },
}

DEFAULT_SCOPES = ("unscoped", "shared_dataset")


@dataclass(frozen=True)
class WarmQuery:
    name: str
    params: dict


def _query_url_from_metadata_url(metadata_url):
    if not metadata_url:
        return None

    parsed = urlsplit(metadata_url)
    path = parsed.path.rstrip("/")
    if path.endswith("/metadata"):
        path = path[: -len("/metadata")] + "/query"
    else:
        path = path.rstrip("/") + "/query"
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def _split_csv(value):
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _resolve_scopes(value):
    scopes = _split_csv(value)
    if not scopes or scopes == ["default"]:
        return list(DEFAULT_SCOPES)
    if scopes == ["all"]:
        return list(SCOPES.keys())

    invalid = sorted(set(scopes) - set(SCOPES))
    if invalid:
        raise ValueError(
            f"Unknown scope(s): {', '.join(invalid)}. "
            f"Expected one of: {', '.join(SCOPES)}"
        )
    return scopes


def _resolve_fields(value):
    fields = _split_csv(value)
    if not fields:
        return list(FILTER_FIELDS)

    invalid = sorted(set(fields) - set(FILTER_FIELDS))
    if invalid:
        raise ValueError(
            f"Unknown field(s): {', '.join(invalid)}. "
            "Use --list-fields to show supported fields."
        )
    return fields


def _date_filter(start_year, end_year):
    return (
        f'(date:["{start_year}-01-01" TO "{end_year}-12-31"] '
        'OR (-_exists_:("date")))'
    )


def _date_clauses(mode, *, start_year, end_year):
    if mode == "none":
        return [""]
    if mode == "default":
        return [_date_filter(start_year, end_year)]
    if mode == "both":
        return ["", _date_filter(start_year, end_year)]
    raise ValueError(f"Unsupported date mode: {mode}")


def _exists_filter(field, exists, *, syntax):
    exists_key = "_exists_" if exists else "-_exists_"
    clause = f'({exists_key}:("{field}"))'
    if syntax == "canonical":
        return clause
    if syntax == "legacy":
        return f"({field}:{clause})"
    raise ValueError(f"Unsupported exists syntax: {syntax}")


def _combine_filters(*filters):
    clauses = [clause for clause in filters if clause]
    return " AND ".join(clauses)


def _facets_for_scope(scope, *, facet_mode):
    scope_fields = SCOPES[scope]["fields"]
    if facet_mode == "all":
        return ",".join(FILTER_FIELDS)
    if facet_mode == "category":
        return ",".join(scope_fields)
    raise ValueError(f"Unsupported facet mode: {facet_mode}")


def _base_params(*, q, facets, facet_size, hist, use_ai_search):
    params = {
        "q": q,
        "size": 0,
        "facet_size": facet_size,
        "facets": facets,
        "use_ai_search": use_ai_search,
    }
    if hist:
        params["hist"] = hist
    return params


def build_warm_queries(
    *,
    q="__all__",
    fields=None,
    scopes=None,
    date_mode="both",
    exists_syntax="legacy",
    facet_mode="all",
    facet_size=1000,
    hist="date",
    use_ai_search="false",
    date_start_year=2000,
    date_end_year=None,
    include_base=True,
):
    fields = list(fields or FILTER_FIELDS)
    scopes = list(scopes or DEFAULT_SCOPES)
    date_end_year = date_end_year or datetime.now().year
    syntax_modes = (
        ["legacy", "canonical"] if exists_syntax == "both" else [exists_syntax]
    )

    for scope in scopes:
        scope_filter = SCOPES[scope]["extra_filter"]
        facets = _facets_for_scope(scope, facet_mode=facet_mode)
        for date_index, date_clause in enumerate(
            _date_clauses(
                date_mode,
                start_year=date_start_year,
                end_year=date_end_year,
            )
        ):
            date_label = "default-date" if date_clause else "no-date"
            if include_base:
                params = _base_params(
                    q=q,
                    facets=facets,
                    facet_size=facet_size,
                    hist=hist,
                    use_ai_search=use_ai_search,
                )
                extra_filter = _combine_filters(date_clause, scope_filter)
                if extra_filter:
                    params["extra_filter"] = extra_filter
                yield WarmQuery(
                    name=f"{scope}:{date_label}:base",
                    params=params,
                )

            for syntax in syntax_modes:
                for field in fields:
                    for exists in (True, False):
                        params = _base_params(
                            q=q,
                            facets=facets,
                            facet_size=facet_size,
                            hist=hist,
                            use_ai_search=use_ai_search,
                        )
                        exists_label = "specified" if exists else "unspecified"
                        extra_filter = _combine_filters(
                            date_clause,
                            _exists_filter(field, exists, syntax=syntax),
                            scope_filter,
                        )
                        params["extra_filter"] = extra_filter
                        yield WarmQuery(
                            name=(
                                f"{scope}:{date_label}:{syntax}:"
                                f"{field}:{exists_label}"
                            ),
                            params=params,
                        )


def _query_url(query_url, params):
    separator = "&" if urlsplit(query_url).query else "?"
    return query_url + separator + urlencode(params)


def _warm_one(query_url, warm_query, *, timeout):
    url = _query_url(query_url, warm_query.params)
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError("Query API response must be a JSON object.")
    if payload.get("success") is False:
        raise ValueError(f"Query API returned failure payload: {payload}")
    return {
        "name": warm_query.name,
        "url": url,
        "total": payload.get("total"),
    }


def warm_filter_cache(query_url, warm_queries, *, timeout=180, max_workers=1):
    stats = {"queued": 0, "completed": 0, "failed": 0}
    warm_queries = list(warm_queries)
    stats["queued"] = len(warm_queries)

    if max_workers <= 1:
        for warm_query in warm_queries:
            try:
                result = _warm_one(query_url, warm_query, timeout=timeout)
                stats["completed"] += 1
                logger.info("Warmed %s total=%s", result["name"], result["total"])
            except Exception:
                stats["failed"] += 1
                logger.warning("Unable to warm %s", warm_query.name, exc_info=True)
        return stats

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_warm_one, query_url, warm_query, timeout=timeout): warm_query
            for warm_query in warm_queries
        }
        for future in as_completed(futures):
            warm_query = futures[future]
            try:
                result = future.result()
                stats["completed"] += 1
                logger.info("Warmed %s total=%s", result["name"], result["total"])
            except Exception:
                stats["failed"] += 1
                logger.warning("Unable to warm %s", warm_query.name, exc_info=True)
    return stats


def build_parser():
    parser = argparse.ArgumentParser(
        description="Warm query API caches for common filter-sidebar aggregations."
    )
    parser.add_argument(
        "--metadata-url",
        default=os.getenv("NDE_METADATA_URL"),
        help="Optional /v1/metadata URL. Used to derive /v1/query.",
    )
    parser.add_argument(
        "--query-url",
        default=os.getenv("NDE_QUERY_URL"),
        help="Required unless --metadata-url is set. Target /v1/query URL.",
    )
    parser.add_argument(
        "--q",
        default="__all__",
        help="Browse/search query to warm. Defaults to portal browse-all.",
    )
    parser.add_argument(
        "--fields",
        default="",
        help="Comma-separated facet fields to warm. Defaults to all known filter fields.",
    )
    parser.add_argument(
        "--scopes",
        default="default",
        help=(
            "Comma-separated scopes to warm. Use 'default' for unscoped and "
            "shared_dataset, or 'all' for every portal scope."
        ),
    )
    parser.add_argument(
        "--exists-syntax",
        choices=("legacy", "canonical", "both"),
        default="legacy",
        help="Exists filter syntax to warm. Legacy matches current HAR URLs.",
    )
    parser.add_argument(
        "--facet-mode",
        choices=("all", "category"),
        default="all",
        help="Facet list per request. Use all for current broad portal requests.",
    )
    parser.add_argument(
        "--date-mode",
        choices=("none", "default", "both"),
        default="both",
        help="Warm no-date, default-date, or both variants.",
    )
    parser.add_argument("--date-start-year", type=int, default=2000)
    parser.add_argument(
        "--date-end-year",
        type=int,
        default=None,
        help="Default date end year. Defaults to the current year.",
    )
    parser.add_argument("--facet-size", type=int, default=1000)
    parser.add_argument("--hist", default="date")
    parser.add_argument("--use-ai-search", default="false")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--no-base",
        action="store_true",
        help="Do not warm the base query without specified/unspecified filters.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print query names and URLs without requesting them.",
    )
    parser.add_argument("--list-fields", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    if args.list_fields:
        for field in FILTER_FIELDS:
            print(field)
        return 0

    query_url = args.query_url or _query_url_from_metadata_url(args.metadata_url)
    if not query_url:
        raise SystemExit("--query-url or --metadata-url is required.")

    try:
        scopes = _resolve_scopes(args.scopes)
        fields = _resolve_fields(args.fields)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    warm_queries = list(
        build_warm_queries(
            q=args.q,
            fields=fields,
            scopes=scopes,
            date_mode=args.date_mode,
            exists_syntax=args.exists_syntax,
            facet_mode=args.facet_mode,
            facet_size=args.facet_size,
            hist=args.hist,
            use_ai_search=args.use_ai_search,
            date_start_year=args.date_start_year,
            date_end_year=args.date_end_year,
            include_base=not args.no_base,
        )
    )
    if args.limit is not None:
        warm_queries = warm_queries[: args.limit]

    logger.info("Prepared %s cache-warming query API requests.", len(warm_queries))

    if args.dry_run:
        for warm_query in warm_queries:
            print(f"{warm_query.name}\t{_query_url(query_url, warm_query.params)}")
        return 0

    stats = warm_filter_cache(
        query_url,
        warm_queries,
        timeout=args.timeout,
        max_workers=args.max_workers,
    )
    logger.info("Finished warming filter cache: %s", stats)
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
