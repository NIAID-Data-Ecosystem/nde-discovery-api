"""Shared input configuration and parsing for staging's repository curation."""

from __future__ import annotations

import csv
import re
import warnings
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RESOURCE_BASE_TSV = REPO_ROOT / "RepoMetaCuration - resource_base.tsv"
SHEET_ID = "12TFTEWZHQECir2fnjUs8-Oc3qcV2VTlCgGq86Dz7wUc"
RESOURCE_BASE_GID = "349233573"
RESOURCE_BASE_SHEET_URL = (
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"
    f"#gid={RESOURCE_BASE_GID}"
)
REQUIRED_COLUMNS = {"name", "url", "abstract", "description"}


def validate_resource_base_tsv(path: Path) -> None:
    with path.open(newline="", encoding="utf-8-sig") as f:
        header = next(csv.reader(f, delimiter="\t"), [])
    if not REQUIRED_COLUMNS.issubset(header):
        raise RuntimeError(
            f"{path} does not look like RepoMetaCuration resource_base.tsv. "
            f"Download the resource_base tab as TSV from {RESOURCE_BASE_SHEET_URL}."
        )


def load_resource_base_rows(path: Path) -> list[dict[str, str]]:
    validate_resource_base_tsv(path)
    rows = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if None in row or None in row.values():
                warnings.warn(
                    f"{path.name}: skipping malformed row ending at line "
                    f"{reader.line_num}: column count does not match the header.",
                    stacklevel=2,
                )
                continue
            if any(value.strip() for value in row.values()):
                rows.append(row)
    return rows


def split_alternate_names(value: str) -> list[str]:
    # Pipes separate the new export's aliases, including names with commas.
    # Older exports used commas or semicolons instead.
    separator = r"\|" if "|" in value else r"[,;]"
    return [
        item.strip(" []\"'")
        for item in re.split(separator, value)
        if item.strip(" []\"'")
    ]


def row_source_candidates(row: dict[str, str]) -> list[str]:
    candidates = [row.get("name", "")]
    candidates.extend(row.get("identifier", "").split("|"))
    candidates.extend(split_alternate_names(row.get("alternateName", "")))
    return [candidate.strip() for candidate in candidates if candidate.strip()]
