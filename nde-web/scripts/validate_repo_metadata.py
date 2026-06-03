#!/usr/bin/env python3
"""Validate repo-level metadata files and generated caches.

This intentionally avoids third-party dependencies so it can run in any
checkout. It checks the project-specific invariants that matter for the
``NDESourceHandler`` metadata path:

* curated source JSON files parse and include required sourceInfo fields
* ``_id`` matches the filename
* common enum / shape fields use the expected local conventions
* heuristic and metadata-completeness cache files parse
* when ``--source`` is provided, generated heuristic/completeness files exist

Usage:
    python nde-web/scripts/validate_repo_metadata.py
    python nde-web/scripts/validate_repo_metadata.py --source uniprot
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
REPO_METADATA_DIR = REPO_ROOT / "nde-web" / "repo_metadata"
HEURISTICS_DIR = REPO_METADATA_DIR / "heuristics"
METADATA_COMPLETENESS_DIR = REPO_ROOT / "nde-web" / "metadata_completeness"

REQUIRED_SOURCE_FIELDS = ("_id", "name", "description", "url", "identifier", "schema")
CONDITIONS_OF_ACCESS = {"Open", "Closed", "Restricted", "Embargoed", "Varied", "Unknown"}
SCHEDULES = {"Weekly", "Monthly", "Quarterly", "Manual"}


def load_json(path: Path, errors: list[str]) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        errors.append(f"{path.relative_to(REPO_ROOT)}: invalid JSON ({exc})")
        return None


def curated_paths(source: str | None) -> list[Path]:
    if source:
        return [REPO_METADATA_DIR / f"{source}.json"]
    return [
        path for path in sorted(REPO_METADATA_DIR.glob("*.json"))
        if not path.name.startswith("_")
    ]


def validate_curated(path: Path, errors: list[str], warnings: list[str]) -> None:
    label = path.relative_to(REPO_ROOT)
    if not path.exists():
        errors.append(f"{label}: missing curated source metadata")
        return
    data = load_json(path, errors)
    if data is None:
        return
    if not isinstance(data, dict):
        errors.append(f"{label}: top-level JSON must be an object")
        return

    key = path.stem
    if data.get("_id") != key:
        errors.append(f"{label}: _id must match filename ({key!r})")

    for field in REQUIRED_SOURCE_FIELDS:
        if field not in data or data[field] in ("", [], {}):
            errors.append(f"{label}: missing required field {field!r}")

    if "schema" in data and not isinstance(data["schema"], dict):
        errors.append(f"{label}: schema must be an object")
    if "genre" in data:
        if not isinstance(data["genre"], list) or not all(
            isinstance(item, str) and item for item in data["genre"]
        ):
            errors.append(f"{label}: genre must be a non-empty string array")
    if data.get("conditionsOfAccess") not in (None, *CONDITIONS_OF_ACCESS):
        errors.append(
            f"{label}: conditionsOfAccess must be one of "
            f"{', '.join(sorted(CONDITIONS_OF_ACCESS))}"
        )
    if data.get("schedule") not in (None, *SCHEDULES):
        errors.append(
            f"{label}: schedule must be one of {', '.join(sorted(SCHEDULES))}"
        )

    if not (HEURISTICS_DIR / f"{key}.json").exists():
        warnings.append(f"{key}: no heuristic cache file")
    if not (METADATA_COMPLETENESS_DIR / f"cache_{key}.json").exists():
        warnings.append(f"{key}: no metadata completeness cache file")


def validate_generated_json(path: Path, errors: list[str]) -> None:
    if not path.exists():
        return
    data = load_json(path, errors)
    if data is not None and not isinstance(data, dict):
        errors.append(f"{path.relative_to(REPO_ROOT)}: top-level JSON must be an object")


def validate_source_generated(source: str, errors: list[str]) -> None:
    required_paths = [
        HEURISTICS_DIR / f"{source}.json",
        METADATA_COMPLETENESS_DIR / f"cache_{source}.json",
    ]
    for path in required_paths:
        if not path.exists():
            errors.append(f"{path.relative_to(REPO_ROOT)}: missing generated file")
        else:
            validate_generated_json(path, errors)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        help="Validate one source and require its generated cache files.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    source = args.source.strip() if args.source else None
    errors: list[str] = []
    warnings: list[str] = []

    for path in curated_paths(source):
        validate_curated(path, errors, warnings)

    if source:
        validate_source_generated(source, errors)
    else:
        for path in sorted(HEURISTICS_DIR.glob("*.json")):
            validate_generated_json(path, errors)
        for path in sorted(METADATA_COMPLETENESS_DIR.glob("cache_*.json")):
            validate_generated_json(path, errors)

    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"  - {warning}")

    if errors:
        print("Errors:")
        for error in errors:
            print(f"  - {error}")
        return 1

    target = source or "all sources"
    print(f"Repo metadata validation passed for {target}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
