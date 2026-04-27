"""Sync per-repo metadata JSON files from curated sources.

Each repo (a NDE data source such as ``ndex``, ``immport``, ``mwccs``) is
represented by a JSON file under ``nde-web/repo_metadata/<key>.json``. The
files are the single source of truth at runtime (loaded by
``NDESourceHandler``). This script regenerates / updates those files by
merging, in order of increasing precedence:

    1. the existing per-repo JSON on disk, if present
    2. the legacy ``source_info`` dict inside ``handlers.py``, if still
       present (used for the initial bootstrap; a no-op afterward)
    3. scalar fields from ``SourceMetaCuration - resource_base.tsv``
       (matched by URL)

Existing fields are preserved; TSV values only fill gaps. Re-run any time
the TSV or Google Sheet changes.

Usage:
    python nde-web/scripts/sync_repo_metadata.py
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
HANDLERS_PY = REPO_ROOT / "nde-web" / "handlers.py"
REPO_METADATA_DIR = REPO_ROOT / "nde-web" / "repo_metadata"
RESOURCE_BASE_TSV = REPO_ROOT / "SourceMetaCuration - resource_base.tsv"
PRIORITY_TSV = REPO_ROOT / "Priority repo metadata - ResourceCatalog.tsv"

# Priority sheet column header -> NDE source key. The sheet has other
# columns (TB Portals, IEDB, ITN TrialShare, ACTG) for repos that are
# not yet ingested; those are ignored.
PRIORITY_SHEET_COLUMNS: dict[str, str] = {
    "ImmuneSpace": "immunespace",
    "BV-BRC": "bv_brc",
    "MACS/WIHS (MWCCS)": "mwccs",
}

# Field order used when writing each JSON file. Fields not listed are
# appended alphabetically at the end. Order mirrors the ResourceCatalog
# schema with NDE-specific fields (schedule, schema, parentCollection) at
# the bottom.
FIELD_ORDER = [
    "_id",
    "name",
    "alternateName",
    "identifier",
    "url",
    "abstract",
    "description",
    "collectionType",
    "type",
    "genre",
    "conditionsOfAccess",
    "license",
    "usageInfo",
    "inLanguage",
    "version",
    "isAccessibleForFree",
    "hasAPI",
    "hasDownload",
    "creativeWorkStatus",
    "dateCreated",
    "dateModified",
    "datePublished",
    "parentCollection",
    "schedule",
    "_mongoCollection",
    "_mongoFilter",
    "schema",
]


def load_source_info_from_handlers() -> dict[str, dict[str, Any]]:
    """Return ``source_info`` if still hardcoded as a dict literal in
    handlers.py, else {}. Used only for the initial bootstrap; after
    the refactor the assignment becomes a function call and this
    returns {}."""
    src = HANDLERS_PY.read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "source_info"
            and isinstance(node.value, ast.Dict)
        ):
            return ast.literal_eval(node.value)
    return {}


def load_existing_repo_jsons() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not REPO_METADATA_DIR.exists():
        return out
    for path in sorted(REPO_METADATA_DIR.glob("*.json")):
        key = path.stem
        with path.open() as f:
            out[key] = json.load(f)
    return out


def _norm_url(url: str) -> str:
    if not url:
        return ""
    u = url.strip().rstrip("/").lower()
    for prefix in ("https://", "http://"):
        if u.startswith(prefix):
            u = u[len(prefix):]
            break
    return u


def _coerce_bool(value: str) -> bool | None:
    v = value.strip().upper()
    if v in ("TRUE", "YES", "1"):
        return True
    if v in ("FALSE", "NO", "0"):
        return False
    return None


# Columns in resource_base.tsv mapped to our JSON field names and how to
# coerce the cell value. A coercer of ``None`` means "use string as-is".
RESOURCE_BASE_COLUMNS: dict[str, tuple[str, Any]] = {
    "name": ("name", None),
    "url": ("url", None),
    "identifier": ("identifier", None),
    "alternateName": (
        "alternateName",
        lambda v: [s.strip() for s in v.split(",") if s.strip()],
    ),
    "license": ("license", None),
    "conditionsOfAccess": ("conditionsOfAccess", None),
    "usageInfo": ("usageInfo", None),
    "abstract": ("abstract", None),
    "description": ("description", None),
    "collectionType": ("collectionType", None),
    "hasAPI": ("hasAPI", _coerce_bool),
    "hasDownload": ("hasDownload", None),
    "isAccessibleForFree": ("isAccessibleForFree", _coerce_bool),
    "inLanguage": ("inLanguage", None),
    "version": ("version", None),
    "genre": ("genre", None),
    "dateModified": ("dateModified", None),
    "dateCreated": ("dateCreated", None),
    "datePublished": ("datePublished", None),
    "creativeWorkStatus": ("creativeWorkStatus", None),
}


# Priority-sheet properties safe to extract as scalars. Complex object
# fields (funding, author, citation, measurementTechnique, infectiousAgent,
# species, variableMeasured, spatialCoverage, temporalCoverage, usageInfo,
# collectionSize, topicCategory, etc.) mix free-text with JSON-like
# fragments — they are left to the Phase 3 heuristics pipeline rather
# than parsed heuristically here.
PRIORITY_SCALAR_PROPERTIES: dict[str, tuple[str, Any]] = {
    "collectionType": ("collectionType", None),
    "genre": ("genre", None),
    "doi": ("doi", None),
    "abstract": ("abstract", None),
    "inLanguage": ("inLanguage", None),
    "version": ("version", None),
    "dateCreated": ("dateCreated", None),
    "dateModified": ("dateModified", None),
    "datePublished": ("datePublished", None),
    "date": ("date", None),
    "hasAPI": ("hasAPI", _coerce_bool),
    "hasDownload": ("hasDownload", None),
    "isAccessibleForFree": ("isAccessibleForFree", _coerce_bool),
    "alternateName": (
        "alternateName",
        lambda v: [
            s.strip(" []\"'")
            for s in v.split(",")
            if s.strip(" []\"'")
        ],
    ),
}


def _clean_date_cell(value: str) -> str | None:
    """Priority-sheet date cells often look like '"2023-04-05"' with
    trailing annotations. Extract the first ISO-ish date if present."""
    import re
    m = re.search(r"(\d{4}-\d{2}-\d{2})", value)
    if m:
        return m.group(1)
    m = re.search(r'"(\d{4})"', value)
    if m:
        return m.group(1)
    return None


def load_priority_sheet_by_key() -> dict[str, dict[str, Any]]:
    """Return priority-sheet scalars keyed by NDE source key.

    The sheet is row-major (one property per row, one column per repo).
    Only the scalar properties in ``PRIORITY_SCALAR_PROPERTIES`` are
    extracted; everything else is left to the heuristics layer.
    """
    if not PRIORITY_TSV.exists():
        return {}
    out: dict[str, dict[str, Any]] = {
        key: {} for key in PRIORITY_SHEET_COLUMNS.values()
    }
    with PRIORITY_TSV.open() as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader)
        header = [h.strip() for h in header]
        col_to_key: dict[int, str] = {}
        for i, h in enumerate(header):
            if h in PRIORITY_SHEET_COLUMNS:
                col_to_key[i] = PRIORITY_SHEET_COLUMNS[h]
        prop_col = header.index("Property") if "Property" in header else 0
        for row in reader:
            if len(row) <= prop_col:
                continue
            prop = row[prop_col].strip()
            if prop not in PRIORITY_SCALAR_PROPERTIES:
                continue
            field, coercer = PRIORITY_SCALAR_PROPERTIES[prop]
            for col_idx, key in col_to_key.items():
                if col_idx >= len(row):
                    continue
                raw = row[col_idx].strip()
                if not raw:
                    continue
                if field in ("dateCreated", "dateModified", "datePublished"):
                    cleaned = _clean_date_cell(raw)
                    if cleaned is None:
                        continue
                    out[key][field] = cleaned
                    continue
                value = coercer(raw) if coercer else raw
                if value is None or value == "" or value == []:
                    continue
                out[key][field] = value
    return {k: v for k, v in out.items() if v}


def load_resource_base_by_url() -> dict[str, dict[str, Any]]:
    """Return resource_base.tsv rows keyed by normalized URL."""
    if not RESOURCE_BASE_TSV.exists():
        return {}
    out: dict[str, dict[str, Any]] = {}
    with RESOURCE_BASE_TSV.open() as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            url = row.get("url", "").strip()
            if not url:
                continue
            record: dict[str, Any] = {}
            for col, (field, coercer) in RESOURCE_BASE_COLUMNS.items():
                raw = (row.get(col) or "").strip()
                if not raw:
                    continue
                value = coercer(raw) if coercer else raw
                if value is None or value == "" or value == []:
                    continue
                record[field] = value
            if record:
                out[_norm_url(url)] = record
    return out


# Fields originally hand-maintained inside handlers.py. These are the
# parity floor: automated TSV syncs may fill them when blank but must
# never overwrite them. Everything else is rebuildable from the sheet.
HANDLERS_PROTECTED_FIELDS = frozenset({
    "name",
    "abstract",
    "description",
    "url",
    "identifier",
    "conditionsOfAccess",
    "genre",
    "schedule",
    "schema",
    "parentCollection",
    "type",
})


def merge(into: dict[str, Any], overlay: dict[str, Any]) -> None:
    """Fill blanks in ``into`` from ``overlay``; never overwrite existing."""
    for key, value in overlay.items():
        if key not in into or into[key] in (None, "", [], {}):
            into[key] = value


def merge_update(into: dict[str, Any], overlay: dict[str, Any]) -> None:
    """Overlay wins for non-protected fields; protected fields are only
    filled when blank. Used when ``overlay`` is authoritative curated
    data (e.g. the priority sheet) that should reflect edits on re-sync.
    """
    for key, value in overlay.items():
        if key in HANDLERS_PROTECTED_FIELDS:
            if key not in into or into[key] in (None, "", [], {}):
                into[key] = value
        else:
            into[key] = value


def order_fields(data: dict[str, Any]) -> dict[str, Any]:
    ordered: dict[str, Any] = {}
    for field in FIELD_ORDER:
        if field in data:
            ordered[field] = data[field]
    for field in sorted(data.keys()):
        if field not in ordered:
            ordered[field] = data[field]
    return ordered


def build() -> dict[str, dict[str, Any]]:
    """Build the merged per-repo metadata dict."""
    repos = load_existing_repo_jsons()
    legacy = load_source_info_from_handlers()
    for key, data in legacy.items():
        repos.setdefault(key, {})
        merge(repos[key], data)
    tsv_by_url = load_resource_base_by_url()
    priority_by_key = load_priority_sheet_by_key()
    # Priority sheet is curated and authoritative for non-protected
    # fields; resource_base.tsv is coarser and only fills blanks.
    for key, data in repos.items():
        data.setdefault("_id", key)
        priority_row = priority_by_key.get(key)
        if priority_row:
            merge_update(data, priority_row)
        url = data.get("url")
        if url:
            tsv_row = tsv_by_url.get(_norm_url(url))
            if tsv_row:
                merge(data, tsv_row)
    return repos


def write(repos: dict[str, dict[str, Any]]) -> list[Path]:
    REPO_METADATA_DIR.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for key, data in sorted(repos.items()):
        ordered = order_fields(data)
        path = REPO_METADATA_DIR / f"{key}.json"
        with path.open("w") as f:
            json.dump(ordered, f, indent=2, ensure_ascii=False)
            f.write("\n")
        written.append(path)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build in memory and print a summary without writing files.",
    )
    args = parser.parse_args(argv)
    repos = build()
    if args.dry_run:
        print(f"Would write {len(repos)} repo metadata files")
        for key in sorted(repos):
            print(f"  {key}: {len(repos[key])} fields")
        return 0
    written = write(repos)
    rel = REPO_METADATA_DIR.relative_to(REPO_ROOT)
    print(f"Wrote {len(written)} files to {rel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
