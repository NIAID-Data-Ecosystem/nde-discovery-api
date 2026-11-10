#!/usr/bin/env python3
"""Add the _identifier key (see user_data.py) to profiles saved before it existed.

Logins keep the key current, so this fills it in for users who haven't signed in
since. NIH Login profiles are left alone: only the login knows which provider NIH
used, and they get the key at their next sign-in.
"""

import argparse
import importlib
import json
import logging
import os
import sys
from pathlib import Path


WEB_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_DIR))

from saved_search_counts import DEFAULT_USER_INDEX  # noqa: E402
from user_data import IDENTIFIER_FIELD, _profile_identifier  # noqa: E402


logger = logging.getLogger(__name__)

SYSTEM_DOC_PREFIX = "_"


def _parse_basic_auth(value):
    if not value:
        return None
    if ":" not in value:
        raise ValueError("--basic-auth must use USER:PASSWORD format")
    username, password = value.split(":", 1)
    return username, password


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


def _is_system_doc(doc_id, source):
    return str(doc_id or "").startswith(SYSTEM_DOC_PREFIX) or bool(
        isinstance(source, dict) and source.get("kind")
    )


def backfill_profile_identifiers(
    client,
    *,
    user_index,
    batch_size,
    scroll,
    dry_run=False,
    limit=None,
):
    stats = {
        "profiles_seen": 0,
        "profiles_current": 0,
        "profiles_updated": 0,
        "profiles_would_update": 0,
        "profiles_without_email": 0,
        "nih_login_profiles_skipped": 0,
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
        if (source.get("oauth_provider") or "").upper() == "OKTA":
            stats["nih_login_profiles_skipped"] += 1
            continue
        # An ORCID profile's username is the user's ORCID iD.
        identifier = _profile_identifier(
            source.get("oauth_provider"), source.get("email"), source.get("username")
        )
        if not identifier:
            stats["profiles_without_email"] += 1
            logger.warning("Skipping profile %s: no email to build %s from", doc_id, IDENTIFIER_FIELD)
            continue
        if source.get(IDENTIFIER_FIELD) == identifier:
            stats["profiles_current"] += 1
            continue

        if dry_run:
            stats["profiles_would_update"] += 1
            logger.info("Dry run: would set %s on %s", IDENTIFIER_FIELD, doc_id)
            continue

        # Only the key changes: bumping `updated` would make a dormant profile
        # look active to delete_inactive_user_profiles.py.
        try:
            client.update(
                index=user_index,
                id=doc_id,
                body={"doc": {IDENTIFIER_FIELD: identifier}},
            )
        except Exception:
            stats["profiles_failed"] += 1
            logger.warning("Unable to set %s on %s", IDENTIFIER_FIELD, doc_id, exc_info=True)
            continue
        stats["profiles_updated"] += 1
        logger.info("Set %s on %s", IDENTIFIER_FIELD, doc_id)

    return stats


def build_parser():
    parser = argparse.ArgumentParser(
        description=f"Add {IDENTIFIER_FIELD} to user profiles saved before it existed."
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
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--scroll", default="5m")
    parser.add_argument("--request-timeout", type=int, default=None)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit user profiles processed.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Count but do not update.")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    args = _apply_config_defaults(args)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    logger.info("Adding %s to user profiles in %s", IDENTIFIER_FIELD, args.user_index)
    client = _build_client(args)
    stats = backfill_profile_identifiers(
        client,
        user_index=args.user_index,
        batch_size=args.batch_size,
        scroll=args.scroll,
        dry_run=args.dry_run,
        limit=args.limit,
    )
    logger.info("Profile %s backfill complete: %s", IDENTIFIER_FIELD, json.dumps(stats))
    return stats


if __name__ == "__main__":
    main()
