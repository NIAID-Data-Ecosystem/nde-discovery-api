#!/usr/bin/env python3
"""Delete user profiles that have been inactive beyond the retention window."""

import argparse
import importlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


WEB_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_DIR))

from saved_search_counts import DEFAULT_USER_INDEX  # noqa: E402


logger = logging.getLogger(__name__)

ACTIVITY_FIELDS = ("last_active", "updated", "created")
SYSTEM_DOC_PREFIX = "_"


def _now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0)


def _years_ago(now, years):
    try:
        return now.replace(year=now.year - years)
    except ValueError:
        return now.replace(year=now.year - years, month=2, day=28)


def _parse_basic_auth(value):
    if not value:
        return None
    if ":" not in value:
        raise ValueError("--basic-auth must use USER:PASSWORD format")
    username, password = value.split(":", 1)
    return username, password


def _parse_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value).strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_config_defaults(config_module=None):
    defaults = {
        "es_host": None,
        "user_index": None,
        "es_args": {},
    }
    module_names = [config_module] if config_module else ["config", "config_web"]
    for module_name in module_names:
        if not module_name:
            continue
        try:
            config = importlib.import_module(module_name)
        except ImportError:
            continue

        defaults.update(
            {
                "es_host": getattr(config, "ES_HOST", None),
                "user_index": getattr(config, "ES_USER_INDEX", None),
                "es_args": dict(getattr(config, "ES_ARGS", {}) or {}),
            }
        )
        break
    return defaults


def _apply_config_defaults(args):
    defaults = _load_config_defaults(args.config_module)
    args.es_host = args.es_host or defaults["es_host"] or "http://localhost:9200"
    args.user_index = args.user_index or defaults["user_index"] or DEFAULT_USER_INDEX
    args.es_args = defaults["es_args"]
    args.request_timeout = (
        args.request_timeout
        or args.es_args.get("request_timeout")
        or 60
    )
    return args


def _build_client(args):
    from elasticsearch import Elasticsearch

    client_kwargs = dict(getattr(args, "es_args", {}) or {})
    client_kwargs["request_timeout"] = args.request_timeout
    if args.api_key:
        client_kwargs["api_key"] = args.api_key
    if args.basic_auth:
        client_kwargs["basic_auth"] = _parse_basic_auth(args.basic_auth)
    if args.ca_certs:
        client_kwargs["ca_certs"] = args.ca_certs
    return Elasticsearch(args.es_host, **client_kwargs)


def _iter_user_profiles(client, *, index, batch_size, scroll):
    from elasticsearch import helpers

    yield from helpers.scan(
        client,
        index=index,
        query={"query": {"match_all": {}}},
        size=batch_size,
        scroll=scroll,
    )


def _profile_last_activity(source):
    if not isinstance(source, dict):
        return None

    for field in ACTIVITY_FIELDS:
        parsed = _parse_datetime(source.get(field))
        if parsed:
            return parsed
    return None


def _resolve_cutoff(*, cutoff_date=None, inactive_years=2, now=None):
    if cutoff_date:
        cutoff = _parse_datetime(cutoff_date)
        if not cutoff:
            raise ValueError("--cutoff-date must be an ISO-8601 datetime or date")
        return cutoff
    return _years_ago(now or _now_utc(), inactive_years)


def _is_system_doc(doc_id, source):
    return str(doc_id or "").startswith(SYSTEM_DOC_PREFIX) or bool(
        isinstance(source, dict) and source.get("kind")
    )


def delete_inactive_user_profiles(
    client,
    *,
    user_index,
    cutoff,
    batch_size,
    scroll,
    dry_run=False,
    limit=None,
):
    cutoff = _parse_datetime(cutoff)
    if cutoff is None:
        raise ValueError("cutoff must be an ISO-8601 datetime or datetime object")

    stats = {
        "profiles_seen": 0,
        "profiles_active": 0,
        "profiles_without_activity": 0,
        "profiles_deleted": 0,
        "profiles_would_delete": 0,
        "profiles_failed": 0,
        "system_docs_skipped": 0,
    }

    for hit in _iter_user_profiles(
        client,
        index=user_index,
        batch_size=batch_size,
        scroll=scroll,
    ):
        doc_id = hit.get("_id")
        source = hit.get("_source") or {}
        if _is_system_doc(doc_id, source):
            stats["system_docs_skipped"] += 1
            continue
        if limit is not None and stats["profiles_seen"] >= limit:
            break

        stats["profiles_seen"] += 1
        last_activity = _profile_last_activity(source)
        if not last_activity:
            stats["profiles_without_activity"] += 1
            logger.warning("Skipping profile %s: no activity timestamp", doc_id)
            continue
        if last_activity >= cutoff:
            stats["profiles_active"] += 1
            continue

        if dry_run:
            stats["profiles_would_delete"] += 1
            logger.info("Dry run: would delete inactive profile %s", doc_id)
            continue

        try:
            client.delete(index=user_index, id=doc_id)
        except Exception:
            stats["profiles_failed"] += 1
            logger.warning("Unable to delete inactive profile %s", doc_id, exc_info=True)
            continue
        stats["profiles_deleted"] += 1
        logger.info("Deleted inactive profile %s", doc_id)

    return stats


def build_parser():
    parser = argparse.ArgumentParser(
        description="Delete user profiles inactive for the retention window."
    )
    parser.add_argument(
        "--config-module",
        default=os.getenv("NDE_CONFIG_MODULE"),
        help="Optional Python config module to read ES defaults from. Defaults to config, then config_web.",
    )
    parser.add_argument(
        "--es-host",
        default=os.getenv("ELASTICSEARCH_URL") or os.getenv("ES_HOST"),
        help="Elasticsearch URL. Defaults to env, config.py, then localhost.",
    )
    parser.add_argument(
        "--user-index",
        default=os.getenv("ES_USER_INDEX"),
        help=f"User profile index. Defaults to config.py or {DEFAULT_USER_INDEX}.",
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
        "--inactive-years",
        type=int,
        default=int(os.getenv("USER_PROFILE_INACTIVE_YEARS", "2")),
        help="Delete profiles inactive for this many calendar years. Defaults to 2.",
    )
    parser.add_argument(
        "--cutoff-date",
        default=os.getenv("USER_PROFILE_RETENTION_CUTOFF"),
        help="Optional ISO cutoff override; profiles active before this instant are deleted.",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--scroll", default="5m")
    parser.add_argument("--request-timeout", type=int, default=None)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit user profiles processed.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Count but do not delete.")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    args = _apply_config_defaults(args)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    cutoff = _resolve_cutoff(
        cutoff_date=args.cutoff_date,
        inactive_years=args.inactive_years,
    )
    logger.info(
        "Deleting user profiles inactive before %s from %s",
        cutoff.isoformat(),
        args.user_index,
    )
    client = _build_client(args)
    stats = delete_inactive_user_profiles(
        client,
        user_index=args.user_index,
        cutoff=cutoff,
        batch_size=args.batch_size,
        scroll=args.scroll,
        dry_run=args.dry_run,
        limit=args.limit,
    )
    logger.info("Inactive user profile cleanup complete: %s", json.dumps(stats))
    return stats


if __name__ == "__main__":
    main()
