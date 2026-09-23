"""Compile production source and DDE ResourceCatalog approvals from RepoMetaCuration.

The private sheet is not deployed. Commit the generated ``exclusions.json``
alongside curated source JSONs so the API can enforce both approval flags.
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
    REPO_ROOT,
    find_resource_base_row,
    load_existing_repo_jsons,
)

EXCLUSIONS_JSON = REPO_ROOT / "nde-web" / "exclusions.json"
VALID_FLAGS = {"TRUE", "FALSE", "IGNORE"}


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource-base-tsv", type=Path, default=RESOURCE_BASE_TSV)
    parser.add_argument("--exclusions-json", type=Path, default=EXCLUSIONS_JSON)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    resource_base = args.resource_base_tsv
    if not resource_base.is_absolute():
        resource_base = REPO_ROOT / resource_base
    try:
        rows = load_resource_base_rows(resource_base)
        current = json.loads(args.exclusions_json.read_text())
        updated = compile_approvals(rows, current, load_existing_repo_jsons())
    except (OSError, RuntimeError, ValueError) as exc:
        parser.error(str(exc))

    old_ids = set(current["staging_ids"])
    new_ids = set(updated["staging_ids"])
    print(
        f"Approved source keys: {len(updated['prod_source_keys'])}; "
        f"newly excluded catalog IDs: {len(new_ids - old_ids)}; "
        f"newly approved catalog IDs: {len(old_ids - new_ids)}"
    )
    if not args.dry_run:
        args.exclusions_json.write_text(json.dumps(updated, indent=4) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
