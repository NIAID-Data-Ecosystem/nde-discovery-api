#!/usr/bin/env python3
"""Bootstrap repo-level metadata artifacts for one NDE source.

For a new source, this helper can:

1. download the latest SourceMetaCuration resource_base Google Sheet as TSV
2. create a minimal ``nde-web/repo_metadata/<source>.json`` stub if needed
3. run ``sync_repo_metadata.py --source <source>``
4. run ``compute_heuristics.py --source <source>``
5. run ``metadata_compatibility_calculator.py --datasource <source>``
6. run ``validate_repo_metadata.py --source <source>``

Usage:
    python nde-web/scripts/bootstrap_source_metadata.py
    python nde-web/scripts/bootstrap_source_metadata.py --source uniprot -y
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
REPO_METADATA_DIR = REPO_ROOT / "nde-web" / "repo_metadata"
METADATA_COMPLETENESS_DIR = REPO_ROOT / "nde-web" / "metadata_completeness"
RESOURCE_BASE_TSV = REPO_ROOT / "SourceMetaCuration - resource_base.tsv"

SHEET_ID = "1SjZ7BNC6oah722psQ_q8oFDB5ZBZjo3np5lBtA3cN-k"
RESOURCE_BASE_GID = "349233573"
RESOURCE_BASE_EXPORT_URL = (
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export"
    f"?format=tsv&gid={RESOURCE_BASE_GID}"
)

DEFAULT_MONGO_URL = (
    "mongodb://su02:27017,su09:27017,su11:27017/"
    "?replicaSet=rs0biothings&readPreference=secondaryPreferred"
)
SCHEDULES = ("Weekly", "Monthly", "Quarterly", "Manual")


def normalize_source_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def prompt(message: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{message}{suffix}: ").strip()
    return value or default or ""


def confirm(message: str, default: bool = True) -> bool:
    suffix = " [Y/n]" if default else " [y/N]"
    value = input(f"{message}{suffix}: ").strip().lower()
    if not value:
        return default
    return value in {"y", "yes"}


def python_for_subprocess() -> str:
    venv_python = REPO_ROOT / "nde-web" / "venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def download_resource_base_tsv(url: str, destination: Path) -> None:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "nde-source-metadata-bootstrap/1.0"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = response.read()

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(payload)
        tmp_path = Path(tmp.name)

    try:
        validate_resource_base_tsv(tmp_path)
        destination.write_bytes(payload)
    finally:
        tmp_path.unlink(missing_ok=True)


def validate_resource_base_tsv(path: Path) -> None:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        try:
            header = next(reader)
        except StopIteration as exc:
            raise RuntimeError(f"{path} is empty") from exc
    required = {"name", "url", "abstract", "description"}
    if not required.issubset(set(header)):
        raise RuntimeError(
            "Downloaded file does not look like SourceMetaCuration "
            "resource_base.tsv. If the sheet requires Google auth, download "
            f"the tab manually as TSV and save it to {RESOURCE_BASE_TSV}."
        )


def load_resource_base_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")
    validate_resource_base_tsv(path)
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def row_source_candidates(row: dict[str, str]) -> list[str]:
    candidates = [
        row.get("name", ""),
        row.get("identifier", ""),
    ]
    alternate = row.get("alternateName", "")
    candidates.extend(re.split(r"[,;]", alternate))
    return [candidate.strip() for candidate in candidates if candidate.strip()]


def find_source_row(rows: list[dict[str, str]], source_key: str) -> dict[str, str] | None:
    for row in rows:
        candidates = row_source_candidates(row)
        if any(normalize_source_key(candidate) == source_key for candidate in candidates):
            return row
    return None


def split_semicolon(value: str) -> list[str]:
    return [item.strip() for item in value.split(";") if item.strip()]


def load_schema(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    schema_path = Path(path).expanduser()
    if not schema_path.is_absolute():
        schema_path = REPO_ROOT / schema_path
    with schema_path.open(encoding="utf-8") as f:
        value = json.load(f)
    if not isinstance(value, dict):
        raise RuntimeError("Schema mapping JSON must be an object")
    return value.get("schema", value)


def create_source_stub(
    source_key: str,
    row: dict[str, str],
    schedule: str,
    schema: dict[str, Any],
    assume_yes: bool,
    dry_run: bool,
) -> None:
    path = REPO_METADATA_DIR / f"{source_key}.json"
    if path.exists():
        return

    name = (row.get("name") or "").strip()
    if not name:
        raise RuntimeError(
            f"Could not create {path}: source row has no name. "
            "Create the source JSON manually or fix the sheet row."
        )

    identifier = (row.get("identifier") or "").strip()
    if not identifier and not assume_yes:
        identifier = prompt("Identifier", name)
    if not identifier:
        identifier = name

    data: dict[str, Any] = {
        "_id": source_key,
        "name": name,
        "identifier": identifier,
        "url": (row.get("url") or "").strip(),
        "abstract": (row.get("abstract") or "").strip(),
        "description": (row.get("description") or "").strip(),
        "schedule": schedule,
        "schema": schema,
    }
    genre = split_semicolon((row.get("genre") or "").strip())
    if genre:
        data["genre"] = genre
    conditions = (row.get("conditionsOfAccess") or "").strip()
    if conditions:
        data["conditionsOfAccess"] = conditions

    data = {k: v for k, v in data.items() if v not in ("", [], {}) or k == "schema"}

    if dry_run:
        print(f"Would create {path.relative_to(REPO_ROOT)}")
        return

    REPO_METADATA_DIR.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"Created {path.relative_to(REPO_ROOT)}")


def run_command(cmd: list[str], dry_run: bool) -> None:
    printable = " ".join(cmd)
    if dry_run:
        print(f"Would run: {printable}")
        return
    print(f"Running: {printable}")
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        help="NDE source key / Mongo collection name, e.g. uniprot.",
    )
    parser.add_argument(
        "--sheet-url",
        default=RESOURCE_BASE_EXPORT_URL,
        help="Google Sheets TSV export URL for SourceMetaCuration resource_base.",
    )
    parser.add_argument(
        "--resource-base-tsv",
        default=str(RESOURCE_BASE_TSV),
        help="Where to save/read SourceMetaCuration - resource_base.tsv.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Use the existing local resource_base TSV instead of downloading it.",
    )
    parser.add_argument(
        "--schema-json",
        help="Optional JSON object containing the source schema mapping.",
    )
    parser.add_argument(
        "--schedule",
        choices=SCHEDULES,
        help="Schedule for a newly-created source JSON stub.",
    )
    parser.add_argument(
        "--mongo-url",
        default=DEFAULT_MONGO_URL,
        help="MongoDB URL for heuristic and completeness generation.",
    )
    parser.add_argument(
        "--mongo-db",
        default="nde_hub_src",
        help="MongoDB database for compute_heuristics.py.",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Do not run sync_repo_metadata.py.",
    )
    parser.add_argument(
        "--skip-heuristics",
        action="store_true",
        help="Do not run compute_heuristics.py.",
    )
    parser.add_argument(
        "--skip-completeness",
        action="store_true",
        help="Do not run metadata_compatibility_calculator.py.",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Do not run validate_repo_metadata.py at the end.",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Use defaults for prompts.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without downloading, writing, or running generators.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    source_input = args.source or prompt("Source key / Mongo collection name")
    source_key = normalize_source_key(source_input)
    if not source_key:
        raise SystemExit("A source key is required.")
    if source_key != source_input:
        print(f"Using normalized source key: {source_key}")

    source_json_path = REPO_METADATA_DIR / f"{source_key}.json"
    resource_base_tsv = Path(args.resource_base_tsv)
    if not resource_base_tsv.is_absolute():
        resource_base_tsv = REPO_ROOT / resource_base_tsv

    should_download = not args.skip_download
    if should_download and not args.yes:
        should_download = confirm(
            "Download the latest SourceMetaCuration resource_base TSV from Google Sheets?",
            default=True,
        )
    if should_download:
        if args.dry_run:
            print(f"Would download {args.sheet_url}")
            print(f"Would save to {resource_base_tsv.relative_to(REPO_ROOT)}")
        else:
            print(f"Downloading {args.sheet_url}")
            download_resource_base_tsv(args.sheet_url, resource_base_tsv)
            print(f"Saved {resource_base_tsv.relative_to(REPO_ROOT)}")

    if resource_base_tsv.exists():
        rows = load_resource_base_rows(resource_base_tsv)
    elif source_json_path.exists():
        rows = []
        print(
            f"Warning: {resource_base_tsv.relative_to(REPO_ROOT)} is missing; "
            "continuing with the existing source JSON only."
        )
    else:
        raise SystemExit(
            f"Missing {resource_base_tsv.relative_to(REPO_ROOT)}. "
            "Download the sheet TSV or rerun without --skip-download."
        )
    row = find_source_row(rows, source_key)
    if row is None and not source_json_path.exists():
        raise SystemExit(
            f"No resource_base row matched source key {source_key!r}. "
            "Add the row to the sheet, or create the source JSON manually "
            "before rerunning."
        )
    if row is None:
        print(
            f"Warning: no resource_base row matched {source_key!r}; "
            "sync will use the existing source JSON only."
        )

    if not source_json_path.exists():
        schedule = args.schedule
        if schedule is None:
            schedule = "Weekly" if args.yes else prompt("Schedule", "Weekly")
        if schedule not in SCHEDULES:
            raise SystemExit(f"Schedule must be one of: {', '.join(SCHEDULES)}")

        schema_path = args.schema_json
        if schema_path is None and not args.yes:
            schema_path = prompt("Schema mapping JSON path (blank for empty schema)", "")
        schema = load_schema(schema_path)

        create_source_stub(
            source_key=source_key,
            row=row or {},
            schedule=schedule,
            schema=schema,
            assume_yes=args.yes,
            dry_run=args.dry_run,
        )

    python = python_for_subprocess()
    if not args.skip_sync:
        run_command(
            [
                python,
                "nde-web/scripts/sync_repo_metadata.py",
                "--source",
                source_key,
            ],
            args.dry_run,
        )
    if not args.skip_heuristics:
        run_command(
            [
                python,
                "nde-web/scripts/compute_heuristics.py",
                "--source",
                source_key,
                "--mongo-url",
                args.mongo_url,
                "--mongo-db",
                args.mongo_db,
                "--verbose",
            ],
            args.dry_run,
        )
    if not args.skip_completeness:
        run_command(
            [
                python,
                "nde-web/scripts/metadata_compatibility_calculator.py",
                "--datasource",
                source_key,
                "--mongo-url",
                args.mongo_url,
                "--cache-dir",
                str(METADATA_COMPLETENESS_DIR.relative_to(REPO_ROOT)),
            ],
            args.dry_run,
        )
    if not args.skip_validation:
        run_command(
            [
                python,
                "nde-web/scripts/validate_repo_metadata.py",
                "--source",
                source_key,
            ],
            args.dry_run,
        )

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
