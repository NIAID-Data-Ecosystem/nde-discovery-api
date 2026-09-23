"""Compile production source and DDE ResourceCatalog approvals from RepoMetaCuration.

The private sheet is not deployed. Commit the generated ``exclusions.json``
alongside curated source JSONs so the API can enforce both approval flags.
Use ``--activate-source-names`` only with a corresponding crawler-record build;
it switches source search identifiers and the production allowlist together.
"""

from __future__ import annotations

import argparse
import json
import re
import warnings
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from resource_base import RESOURCE_BASE_TSV, load_resource_base_rows
from sync_repo_metadata import (
    REPO_METADATA_DIR,
    REPO_ROOT,
    find_resource_base_row,
    load_existing_repo_jsons,
    order_fields,
)

EXCLUSIONS_JSON = REPO_ROOT / "nde-web" / "exclusions.json"
VALID_FLAGS = {"TRUE", "FALSE", "IGNORE"}
QUERY_NAME_PARENT_KEYS = {"veupath_collections": "veupathdb"}


def catalog_id(same_as: str) -> str | None:
    """Return the DDE record ID referenced by a resource-page link."""
    parsed = urlsplit(same_as or "")
    if parsed.path.rstrip("/") != "/resources":
        return None
    values = parse_qs(parsed.query).get("id", [])
    if len(values) == 1 and re.fullmatch(r"dde_[0-9a-f]+", values[0]):
        return values[0]
    return None


def compile_approvals(rows: list[dict[str, str]], exclusions: dict,
                      repos: dict[str, dict]) -> dict:
    """Return an exclusions copy with source and catalog approval gates."""
    existing_ids = exclusions.get("staging_ids")
    if not isinstance(existing_ids, list) or not all(
        isinstance(item, str) for item in existing_ids
    ):
        raise ValueError("exclusions.json requires a staging_ids string list")
    if not isinstance(exclusions.get("prod_catalogs"), list):
        raise ValueError("exclusions.json requires prod_catalogs")

    catalog_flags: dict[str, str] = {}
    for row in rows:
        name = row.get("name", "").strip()
        if not name:
            continue
        for field in ("_ProdApproved?", "_ResCatProdApproved?"):
            if row.get(field) not in VALID_FLAGS:
                raise ValueError(f"{name}: invalid or missing {field}")
        record_id = catalog_id(row.get("sameAs", ""))
        if row["_ResCatProdApproved?"] == "TRUE" and not record_id:
            raise ValueError(f"{name}: approved catalog has no DDE sameAs ID")
        if record_id:
            flag = row["_ResCatProdApproved?"]
            if record_id in catalog_flags and catalog_flags[record_id] != flag:
                raise ValueError(f"{record_id}: conflicting catalog approvals")
            catalog_flags[record_id] = flag

    approved_keys = set()
    matched_approved_rows = set()
    for key, source in repos.items():
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            row = find_resource_base_row(rows, key, source)
        if row and row["_ProdApproved?"] == "TRUE":
            approved_keys.add(key)
            matched_approved_rows.add(id(row))

    missing_sources = [
        row["name"] for row in rows
        if row.get("_ProdApproved?") == "TRUE" and id(row) not in matched_approved_rows
    ]
    if missing_sources:
        raise ValueError(
            "approved sources have no matching repo metadata JSON: "
            + ", ".join(missing_sources)
        )

    approved_catalogs = {key for key, flag in catalog_flags.items() if flag == "TRUE"}
    unapproved_catalogs = set(catalog_flags) - approved_catalogs
    staging_ids = [item for item in existing_ids if item not in approved_catalogs]
    staging_ids.extend(sorted(unapproved_catalogs - set(staging_ids)))
    return {
        **exclusions,
        "staging_ids": staging_ids,
        "prod_source_keys": sorted(approved_keys),
        "prod_resource_catalog_ids": sorted(approved_catalogs),
    }


def activate_source_names(
    rows: list[dict[str, str]], exclusions: dict, repos: dict[str, dict]
) -> tuple[dict, dict[str, dict]]:
    """Synchronize approved search names with source identifiers and allowlist.

    The sheet's ``identifier`` column contains URLs and aliases, so its
    single display ``name`` is the exact query name. VEuPath Collections is
    the one source whose records are intentionally scoped to its parent.
    """
    approved_rows = {}
    for key, source in repos.items():
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            row = find_resource_base_row(rows, key, source)
        if row and row["_ProdApproved?"] == "TRUE":
            approved_rows[key] = row

    renamed_catalogs: dict[str, str] = {}
    updated_repos: dict[str, dict] = {}
    for key, row in approved_rows.items():
        parent_key = QUERY_NAME_PARENT_KEYS.get(key, key)
        parent_row = approved_rows.get(parent_key)
        if parent_row is None:
            raise ValueError(
                f"{key}: approved query-name parent {parent_key!r} is missing"
            )
        new_name = parent_row.get("name", "").strip()
        if not new_name:
            raise ValueError(f"{key}: approved query name is blank")
        old_name = repos[key].get("identifier")
        if not isinstance(old_name, str) or not old_name:
            raise ValueError(f"{key}: existing source identifier is missing")
        if old_name in renamed_catalogs and renamed_catalogs[old_name] != new_name:
            raise ValueError(f"{old_name}: conflicting approved query names")
        renamed_catalogs[old_name] = new_name
        if old_name != new_name:
            updated_repos[key] = {**repos[key], "identifier": new_name}

    prod_catalogs = []
    for old_name in exclusions["prod_catalogs"]:
        new_name = renamed_catalogs.get(old_name, old_name)
        if new_name not in prod_catalogs:
            prod_catalogs.append(new_name)
    return {**exclusions, "prod_catalogs": prod_catalogs}, updated_repos


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource-base-tsv", type=Path, default=RESOURCE_BASE_TSV)
    parser.add_argument("--exclusions-json", type=Path, default=EXCLUSIONS_JSON)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--activate-source-names",
        action="store_true",
        help="Switch approved source identifiers and prod_catalogs to sheet names together.",
    )
    args = parser.parse_args(argv)
    resource_base = args.resource_base_tsv
    if not resource_base.is_absolute():
        resource_base = REPO_ROOT / resource_base
    try:
        rows = load_resource_base_rows(resource_base)
        current = json.loads(args.exclusions_json.read_text())
        repos = load_existing_repo_jsons()
        updated = compile_approvals(rows, current, repos)
        source_updates = {}
        if args.activate_source_names:
            updated, source_updates = activate_source_names(rows, updated, repos)
    except (OSError, RuntimeError, ValueError) as exc:
        parser.error(str(exc))

    old_ids = set(current["staging_ids"])
    new_ids = set(updated["staging_ids"])
    print(
        f"Approved source keys: {len(updated['prod_source_keys'])}; "
        f"newly excluded catalog IDs: {len(new_ids - old_ids)}; "
        f"newly approved catalog IDs: {len(old_ids - new_ids)}; "
        f"renamed source identifiers: {len(source_updates)}"
    )
    if not args.dry_run:
        for key, source in source_updates.items():
            path = REPO_METADATA_DIR / f"{key}.json"
            path.write_text(
                json.dumps(order_fields(source), indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
        args.exclusions_json.write_text(json.dumps(updated, indent=4) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
