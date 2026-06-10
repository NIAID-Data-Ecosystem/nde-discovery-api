#!/usr/bin/env python3
"""Bootstrap repo-level metadata artifacts for NDE sources.

For one source, this helper can:

1. prompt for the private SourceMetaCuration resource_base Google Sheet TSV
2. create a minimal ``nde-web/repo_metadata/<source>.json`` stub if needed
3. run ``sync_repo_metadata.py --source <source>``
4. run ``compute_heuristics.py --source <source>``
5. run ``metadata_compatibility_calculator.py --datasource <source>``
6. run ``validate_repo_metadata.py --source <source>``

Usage:
    python nde-web/scripts/bootstrap_source_metadata.py
    python nde-web/scripts/bootstrap_source_metadata.py --source uniprot -y
    python nde-web/scripts/bootstrap_source_metadata.py --all -y
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
REPO_METADATA_DIR = REPO_ROOT / "nde-web" / "repo_metadata"
METADATA_COMPLETENESS_DIR = REPO_ROOT / "nde-web" / "metadata_completeness"
RESOURCE_BASE_TSV = REPO_ROOT / "SourceMetaCuration - resource_base.tsv"

SHEET_ID = "1SjZ7BNC6oah722psQ_q8oFDB5ZBZjo3np5lBtA3cN-k"
RESOURCE_BASE_GID = "349233573"
RESOURCE_BASE_SHEET_URL = (
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"
    f"#gid={RESOURCE_BASE_GID}"
)

DEFAULT_MONGO_URL = (
    "mongodb://su02:27017,su09:27017,su11:27017/"
    "?replicaSet=rs0biothings&readPreference=secondaryPreferred"
)


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


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def resource_base_download_instructions(path: Path) -> str:
    return "\n".join(
        [
            "The SourceMetaCuration resource_base sheet is private, so this "
            "script cannot download it automatically.",
            f"Open the sheet: {RESOURCE_BASE_SHEET_URL}",
            "Select the resource_base tab, then choose File > Download > "
            "Tab-separated values (.tsv, current sheet).",
            f"Move/rename the downloaded file to: {display_path(path)}",
        ]
    )


def prompt_for_resource_base_tsv(
    path: Path,
    dry_run: bool,
    wait_for_replacement: bool = False,
) -> None:
    print(resource_base_download_instructions(path))
    if dry_run:
        print(f"Would wait for {display_path(path)} to exist.")
        return

    if wait_for_replacement and path.exists():
        input("Press Enter after replacing the TSV, or Ctrl-C to stop.")

    while not path.exists():
        input("Press Enter after moving the TSV into place, or Ctrl-C to stop.")
        if not path.exists():
            print(f"Still missing {display_path(path)}.")


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
            f"{display_path(path)} does not look like SourceMetaCuration "
            "resource_base.tsv. "
            f"Download the resource_base tab as TSV from {RESOURCE_BASE_SHEET_URL} "
            f"and save it to {display_path(RESOURCE_BASE_TSV)}."
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


def source_stub_reminder(path: Path) -> str:
    return (
        f"Reminder: fill out 'schedule' and 'schema' in {display_path(path)} "
        "before committing the source metadata."
    )


def create_source_stub(
    source_key: str,
    row: dict[str, str],
    assume_yes: bool,
    dry_run: bool,
) -> Path | None:
    path = REPO_METADATA_DIR / f"{source_key}.json"
    if path.exists():
        return None

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
        "schedule": "",
        "schema": {},
    }
    genre = split_semicolon((row.get("genre") or "").strip())
    if genre:
        data["genre"] = genre
    conditions = (row.get("conditionsOfAccess") or "").strip()
    if conditions:
        data["conditionsOfAccess"] = conditions

    data = {
        k: v
        for k, v in data.items()
        if v not in ("", [], {}) or k in {"schedule", "schema"}
    }

    if dry_run:
        print(f"Would create {path.relative_to(REPO_ROOT)}")
        return path

    REPO_METADATA_DIR.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"Created {path.relative_to(REPO_ROOT)}")
    return path


def run_command(cmd: list[str], dry_run: bool) -> None:
    printable = " ".join(cmd)
    if dry_run:
        print(f"Would run: {printable}")
        return
    print(f"Running: {printable}")
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)


def existing_source_keys() -> list[str]:
    """Return source keys with existing curated metadata JSON files."""
    if not REPO_METADATA_DIR.exists():
        return []
    return [
        path.stem
        for path in sorted(REPO_METADATA_DIR.glob("*.json"))
        if not path.name.startswith("_")
    ]


def resolve_resource_base_tsv(path_arg: str) -> Path:
    path = Path(path_arg)
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path


def prepare_resource_base_tsv(
    args: argparse.Namespace,
    resource_base_tsv: Path,
    require_for_new_source: bool,
) -> list[dict[str, str]]:
    if not args.skip_download:
        if args.yes:
            if not resource_base_tsv.exists():
                print(resource_base_download_instructions(resource_base_tsv))
        elif resource_base_tsv.exists():
            if confirm(
                "Refresh SourceMetaCuration resource_base TSV manually before continuing?",
                default=False,
            ):
                prompt_for_resource_base_tsv(
                    resource_base_tsv,
                    args.dry_run,
                    wait_for_replacement=True,
                )
        else:
            print(f"Missing {display_path(resource_base_tsv)}.")
            prompt_for_resource_base_tsv(resource_base_tsv, args.dry_run)

    if resource_base_tsv.exists():
        return load_resource_base_rows(resource_base_tsv)
    if require_for_new_source:
        raise SystemExit(
            f"Missing {display_path(resource_base_tsv)}. "
            f"Download the resource_base tab as TSV from {RESOURCE_BASE_SHEET_URL} "
            "and move it to the repository root before rerunning."
        )
    print(
        f"Warning: {display_path(resource_base_tsv)} is missing; "
        "continuing with existing source JSON files only."
    )
    return []


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        help="NDE source key / Mongo collection name, e.g. uniprot.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Bootstrap every existing source in nde-web/repo_metadata.",
    )
    parser.add_argument(
        "--resource-base-tsv",
        default=str(RESOURCE_BASE_TSV),
        help="Where to save/read SourceMetaCuration - resource_base.tsv.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help=argparse.SUPPRESS,
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
        help="Print actions without writing or running generators.",
    )
    args = parser.parse_args(argv)
    if args.all and args.source:
        parser.error("--all cannot be used with --source")
    return args


def bootstrap_source(
    args: argparse.Namespace,
    source_key: str,
    rows: list[dict[str, str]],
) -> None:
    source_json_path = REPO_METADATA_DIR / f"{source_key}.json"

    row = find_source_row(rows, source_key)
    if row is None and not source_json_path.exists():
        raise SystemExit(
            f"No resource_base row matched source key {source_key!r}. "
            "Add the row to the sheet, or create the source JSON manually "
            "before rerunning."
        )
    if row is None:
        print(
            f"Warning: no resource_base row matched {source_key!r} by "
            "source key; sync_repo_metadata.py may still match by URL."
        )

    created_stub_path = None
    if not source_json_path.exists():
        created_stub_path = create_source_stub(
            source_key=source_key,
            row=row or {},
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
    if created_stub_path is not None:
        print(source_stub_reminder(created_stub_path))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    resource_base_tsv = resolve_resource_base_tsv(args.resource_base_tsv)

    if args.all:
        source_keys = existing_source_keys()
        if not source_keys:
            raise SystemExit(f"No source JSON files found in {display_path(REPO_METADATA_DIR)}.")
        rows = prepare_resource_base_tsv(
            args,
            resource_base_tsv,
            require_for_new_source=False,
        )
        total = len(source_keys)
        for index, source_key in enumerate(source_keys, start=1):
            print(f"\n=== [{index}/{total}] {source_key} ===")
            bootstrap_source(args, source_key, rows)
        if not args.skip_validation:
            run_command(
                [
                    python_for_subprocess(),
                    "nde-web/scripts/validate_repo_metadata.py",
                ],
                args.dry_run,
            )
        print(f"Done bootstrapping {total} sources.")
        return 0

    source_input = args.source or prompt("Source key / Mongo collection name")
    source_key = normalize_source_key(source_input)
    if not source_key:
        raise SystemExit("A source key is required.")
    if source_key != source_input:
        print(f"Using normalized source key: {source_key}")

    source_json_path = REPO_METADATA_DIR / f"{source_key}.json"
    rows = prepare_resource_base_tsv(
        args,
        resource_base_tsv,
        require_for_new_source=not source_json_path.exists(),
    )
    bootstrap_source(args, source_key, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
