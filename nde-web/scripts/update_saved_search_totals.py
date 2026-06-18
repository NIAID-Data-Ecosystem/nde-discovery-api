#!/usr/bin/env python3
"""Refresh stored result totals for user saved searches.

Run this after publishing a new data release so each user profile's
``favorite_searches[*].total`` reflects the current search index.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen


WEB_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_DIR))

from saved_search_counts import (  # noqa: E402
    DEFAULT_DATA_INDEX,
    DEFAULT_USER_INDEX,
    build_saved_search_count_body,
)


logger = logging.getLogger(__name__)

REFRESH_MARKER_ID = "_saved_search_totals_refresh"


def _now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_basic_auth(value):
    if not value:
        return None
    if ":" not in value:
        raise ValueError("--basic-auth must use USER:PASSWORD format")
    username, password = value.split(":", 1)
    return username, password


def _build_client(args):
    from elasticsearch import Elasticsearch

    client_kwargs = {"request_timeout": args.request_timeout}
    if args.api_key:
        client_kwargs["api_key"] = args.api_key
    if args.basic_auth:
        client_kwargs["basic_auth"] = _parse_basic_auth(args.basic_auth)
    if args.ca_certs:
        client_kwargs["ca_certs"] = args.ca_certs
    return Elasticsearch(args.es_host, **client_kwargs)


def _extract_build_info(metadata):
    if not isinstance(metadata, dict):
        return {}

    return {
        key: metadata[key]
        for key in ("biothing_type", "build_date", "build_version")
        if metadata.get(key)
    }


def _fetch_build_info(metadata_url, *, timeout):
    request = Request(metadata_url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        return _extract_build_info(json.load(response))


def _resolve_build_info(args):
    build_info = {}
    if args.metadata_url:
        build_info.update(
            _fetch_build_info(args.metadata_url, timeout=args.request_timeout)
        )
    if args.build_date:
        build_info["build_date"] = args.build_date
    if args.build_version:
        build_info["build_version"] = args.build_version
    return build_info


def _is_not_found_error(exc):
    if getattr(exc, "status_code", None) == 404:
        return True
    meta = getattr(exc, "meta", None)
    if getattr(meta, "status", None) == 404:
        return True
    return exc.__class__.__name__ == "NotFoundError"


def _get_refresh_marker(client, *, user_index):
    try:
        response = client.get(index=user_index, id=REFRESH_MARKER_ID)
    except Exception as exc:
        if _is_not_found_error(exc):
            return None
        raise
    return response.get("_source") or {}


def _build_marker_matches(marker, build_info):
    release_keys = {
        key: value
        for key, value in build_info.items()
        if key in {"build_date", "build_version"} and value
    }
    if not marker or not release_keys:
        return False
    return all(marker.get(key) == value for key, value in release_keys.items())


def _save_refresh_marker(client, *, user_index, build_info, stats):
    body = {
        "kind": "saved_search_totals_refresh",
        "updated": _now_iso(),
        "stats": stats,
    }
    body.update(build_info)
    client.index(index=user_index, id=REFRESH_MARKER_ID, body=body)


def _iter_user_profiles(client, *, index, batch_size, scroll):
    from elasticsearch import helpers

    yield from helpers.scan(
        client,
        index=index,
        query={"query": {"match_all": {}}},
        size=batch_size,
        scroll=scroll,
    )


def _count_saved_search(client, *, index, favorite_search):
    body = build_saved_search_count_body(
        favorite_search.get("query"),
        favorite_search.get("filters"),
    )
    response = client.count(index=index, query=body["query"])
    return int(response["count"])


def refresh_saved_search_totals(
    client,
    *,
    user_index,
    data_index,
    batch_size,
    scroll,
    dry_run=False,
    limit=None,
):
    stats = {
        "profiles_seen": 0,
        "profiles_changed": 0,
        "saved_searches_seen": 0,
        "saved_searches_changed": 0,
        "saved_searches_failed": 0,
    }

    for hit in _iter_user_profiles(
        client,
        index=user_index,
        batch_size=batch_size,
        scroll=scroll,
    ):
        if limit is not None and stats["profiles_seen"] >= limit:
            break

        doc_id = hit.get("_id")
        if doc_id == REFRESH_MARKER_ID:
            continue

        stats["profiles_seen"] += 1
        source = hit.get("_source") or {}
        favorite_searches = source.get("favorite_searches") or []
        if not isinstance(favorite_searches, list):
            logger.warning(
                "Skipping profile %s: favorite_searches is not a list",
                doc_id,
            )
            continue

        changed = False
        for search_index, favorite_search in enumerate(favorite_searches):
            if not isinstance(favorite_search, dict):
                logger.warning(
                    "Skipping profile %s saved search %s: entry is not an object",
                    doc_id,
                    search_index,
                )
                continue

            stats["saved_searches_seen"] += 1
            try:
                total = _count_saved_search(
                    client,
                    index=data_index,
                    favorite_search=favorite_search,
                )
            except Exception:
                stats["saved_searches_failed"] += 1
                logger.warning(
                    "Unable to count profile %s saved search %s",
                    doc_id,
                    search_index,
                    exc_info=True,
                )
                continue

            if favorite_search.get("total") != total:
                favorite_search["total"] = total
                stats["saved_searches_changed"] += 1
                changed = True

        if not changed:
            continue

        stats["profiles_changed"] += 1
        if dry_run:
            logger.info("Dry run: would update profile %s", doc_id)
            continue

        client.update(
            index=user_index,
            id=doc_id,
            body={
                "doc": {
                    "favorite_searches": favorite_searches,
                    "updated": _now_iso(),
                }
            },
        )
        logger.info("Updated profile %s", doc_id)

    return stats


def build_parser():
    parser = argparse.ArgumentParser(
        description="Refresh favorite_searches[*].total in user profile data."
    )
    parser.add_argument(
        "--es-host",
        default=os.getenv("ELASTICSEARCH_URL")
        or os.getenv("ES_HOST")
        or "http://localhost:9200",
        help="Elasticsearch URL. Defaults to ELASTICSEARCH_URL, ES_HOST, or localhost.",
    )
    parser.add_argument(
        "--user-index",
        default=os.getenv("ES_USER_INDEX", DEFAULT_USER_INDEX),
        help=f"User profile index. Defaults to {DEFAULT_USER_INDEX}.",
    )
    parser.add_argument(
        "--data-index",
        default=os.getenv("ES_DATA_INDEX", DEFAULT_DATA_INDEX),
        help=f"Data search index. Defaults to {DEFAULT_DATA_INDEX}.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("ELASTICSEARCH_API_KEY") or os.getenv("ES_API_KEY"),
        help="Optional Elasticsearch API key.",
    )
    parser.add_argument(
        "--basic-auth",
        default=os.getenv("ELASTICSEARCH_BASIC_AUTH") or os.getenv("ES_BASIC_AUTH"),
        help="Optional Elasticsearch basic auth in USER:PASSWORD format.",
    )
    parser.add_argument(
        "--ca-certs",
        default=os.getenv("ELASTICSEARCH_CA_CERTS") or os.getenv("ES_CA_CERTS"),
        help="Optional CA bundle path.",
    )
    parser.add_argument(
        "--metadata-url",
        default=os.getenv("NDE_METADATA_URL"),
        help="Optional /v1/metadata URL used to detect the current build.",
    )
    parser.add_argument(
        "--build-date",
        default=os.getenv("NDE_BUILD_DATE"),
        help="Optional build date override when metadata URL is not available.",
    )
    parser.add_argument(
        "--build-version",
        default=os.getenv("NDE_BUILD_VERSION"),
        help="Optional build version override when metadata URL is not available.",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--scroll", default="5m")
    parser.add_argument("--request-timeout", type=int, default=60)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit profiles processed.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Count but do not write.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Refresh even if this build was already marked complete.",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    client = _build_client(args)
    build_info = _resolve_build_info(args)
    if build_info:
        logger.info("Resolved build info: %s", build_info)

    if build_info and not args.force:
        marker = _get_refresh_marker(client, user_index=args.user_index)
        if _build_marker_matches(marker, build_info):
            logger.info(
                "Saved search totals already refreshed for this build: %s",
                build_info,
            )
            return 0

    stats = refresh_saved_search_totals(
        client,
        user_index=args.user_index,
        data_index=args.data_index,
        batch_size=args.batch_size,
        scroll=args.scroll,
        dry_run=args.dry_run,
        limit=args.limit,
    )
    if build_info and not args.dry_run:
        if stats["saved_searches_failed"]:
            logger.warning(
                "Not marking build complete because %s saved searches failed.",
                stats["saved_searches_failed"],
            )
        else:
            _save_refresh_marker(
                client,
                user_index=args.user_index,
                build_info=build_info,
                stats=stats,
            )

    logger.info("Finished refreshing saved search totals: %s", stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
