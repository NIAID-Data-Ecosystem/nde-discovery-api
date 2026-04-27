"""Compute heuristic repo-level metadata from indexed records.

Implements the rules from ``SourceMetaCuration - heuristics.tsv``. For
each source in ``nde-web/repo_metadata/``, aggregates per-record data
in MongoDB (``nde_hub_src.<source_key>``) into repo-level fields:

* ``license``, ``usageInfo`` — consensus across records; ``"Varies"``
  when multiple distinct values are present
* ``dateModified`` — max across records
* ``collectionSize`` — per ``@type`` count as a ``QuantitativeValue``
* ``temporalCoverage`` — ``TemporalInterval`` from earliest
  ``dateCreated``/``datePublished`` to latest ``dateModified``
* ``spatialCoverage`` — deduped ``AdministrativeArea`` array
* ``topicCategory``, ``healthCondition``, ``species``,
  ``infectiousAgent``, ``measurementTechnique``, ``variableMeasured`` —
  deduped ``DefinedTerm`` arrays

Results are written to ``nde-web/repo_metadata/heuristics/<key>.json``
and loaded by ``NDESourceHandler`` at runtime. Curated values in
``<key>.json`` always win over heuristic values — the cache is purely
additive.

Usage:
    # default: mongodb://su11:27017/ nde_hub_src
    python nde-web/scripts/compute_heuristics.py
    python nde-web/scripts/compute_heuristics.py --source ndex
    python nde-web/scripts/compute_heuristics.py --dry-run

Env vars: MONGO_URL (default mongodb://su11:27017/),
          MONGO_DB  (default nde_hub_src).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
REPO_METADATA_DIR = REPO_ROOT / "nde-web" / "repo_metadata"
HEURISTICS_DIR = REPO_METADATA_DIR / "heuristics"

DEFAULT_MONGO_URL = os.environ.get(
    "MONGO_URL", "mongodb://su11:27017/"
)
DEFAULT_MONGO_DB = os.environ.get("MONGO_DB", "nde_hub_src")

# Cap for the number of distinct values kept per aggregated field.
# Protects against runaway arrays on pathological sources.
MAX_TERMS = int(os.environ.get("HEURISTICS_MAX_TERMS", "5000"))

DEFINED_TERM_FIELDS = [
    "topicCategory",
    "healthCondition",
    "species",
    "infectiousAgent",
    "measurementTechnique",
    # variableMeasured intentionally excluded per updated
    # SourceMetaCuration - heuristics.tsv: "not standardized and
    # therefore difficult to de-duplicate".
]

# Per-field rollup term appended when a DefinedTerm array exceeds
# ``DEFINED_TERM_TOP_N``. Labels/identifiers come from the curation doc.
DEFINED_TERM_TOP_N = int(os.environ.get("HEURISTICS_TOP_N", "10"))

DEFINED_TERM_ROLLUP = {
    "healthCondition": {
        "@type": "DefinedTerm",
        "name": "Other",
        "url": "http://purl.obolibrary.org/obo/NCIT_C17649",
        "inDefinedTermSet": "NCIT",
        "identifier": "C17649",
        "termCode": "NCIT_C17649",
    },
    "measurementTechnique": {
        "@type": "DefinedTerm",
        "name": "Other",
        "url": "http://purl.obolibrary.org/obo/NCIT_C17649",
        "inDefinedTermSet": "NCIT",
        "identifier": "C17649",
        "termCode": "NCIT_C17649",
    },
    "infectiousAgent": {
        "@type": "DefinedTerm",
        "name": "All",
        "url": "http://purl.obolibrary.org/obo/NCBITaxon_1",
        "inDefinedTermSet": "NCBITaxon",
        "identifier": "1",
        "termCode": "NCBITaxon_1",
    },
    "species": {
        "@type": "DefinedTerm",
        "name": "All",
        "url": "http://purl.obolibrary.org/obo/NCBITaxon_1",
        "inDefinedTermSet": "NCBITaxon",
        "identifier": "1",
        "termCode": "NCBITaxon_1",
    },
    "topicCategory": {
        "@type": "DefinedTerm",
        "name": "Topic",
        "url": "http://edamontology.org/topic_0003",
        "inDefinedTermSet": "EDAM",
        "identifier": "0003",
        "termCode": "topic_0003",
    },
}


# ---------------------------------------------------------------------------
# Aggregation builders (one per heuristic, for clarity & testability)
# ---------------------------------------------------------------------------

def agg_collection_size() -> list[dict[str, Any]]:
    """Count docs per ``@type``; ``@type`` needs $getField because its
    name starts with ``@`` (invalid in a Mongo field path)."""
    return [
        {
            "$group": {
                "_id": {"$getField": "@type"},
                "count": {"$sum": 1},
            }
        }
    ]


def agg_dates() -> list[dict[str, Any]]:
    """Min dateCreated/datePublished + max dateModified in one pass."""
    return [
        {
            "$group": {
                "_id": None,
                "min_dateCreated": {"$min": "$dateCreated"},
                "min_datePublished": {"$min": "$datePublished"},
                "max_dateModified": {"$max": "$dateModified"},
            }
        }
    ]


def agg_consensus(field: str) -> list[dict[str, Any]]:
    """Distinct values of a scalar field so we can decide
    single-value vs ``Varies``. Works for string fields like
    ``license`` and for object fields like ``usageInfo`` (we group on
    the whole object)."""
    return [
        {"$match": {field: {"$ne": None}}},
        {"$group": {"_id": f"${field}"}},
        {"$limit": 2},  # only need to know "1" vs ">1"
    ]


def agg_defined_term(field: str) -> list[dict[str, Any]]:
    """Group records by ``(identifier, name)`` and count frequency so
    the caller can keep only the top-N most common terms. Returns
    representative ``url`` / ``inDefinedTermSet`` / ``termCode`` values
    via ``$first`` so the emitted DefinedTerm matches the DB shape."""
    return [
        {"$unwind": f"${field}"},
        {
            "$group": {
                "_id": {
                    "identifier": f"${field}.identifier",
                    "name": f"${field}.name",
                },
                "count": {"$sum": 1},
                "url": {"$first": f"${field}.url"},
                "inDefinedTermSet": {
                    "$first": f"${field}.inDefinedTermSet"
                },
                "termCode": {"$first": f"${field}.termCode"},
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": MAX_TERMS},
    ]


def agg_spatial_coverage() -> list[dict[str, Any]]:
    """Unique ``spatialCoverage.name`` values."""
    return [
        {"$unwind": "$spatialCoverage"},
        {
            "$group": {
                "_id": "$spatialCoverage.name",
            }
        },
        {"$match": {"_id": {"$ne": None}}},
        {"$limit": MAX_TERMS},
    ]


# ---------------------------------------------------------------------------
# Heuristic computation for one source
# ---------------------------------------------------------------------------

def _normalize_date(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value)
    # Trim ISO timestamps to the date portion.
    if "T" in s:
        s = s.split("T", 1)[0]
    return s or None


def _apply_top_n_and_rollup(
    terms: list[dict[str, Any]],
    field: str,
) -> list[dict[str, Any]]:
    """Keep at most ``DEFINED_TERM_TOP_N`` terms. If the list was longer
    before capping, append the per-field rollup DefinedTerm. Any rollup
    already present in the input is stripped first so merging never
    produces duplicate rollups."""
    rollup = DEFINED_TERM_ROLLUP.get(field)
    if rollup:
        rollup_sig = (rollup.get("identifier"), rollup.get("name"))
        terms = [
            t for t in terms
            if (t.get("identifier"), t.get("name")) != rollup_sig
        ]
    if len(terms) > DEFINED_TERM_TOP_N:
        terms = terms[:DEFINED_TERM_TOP_N]
        if rollup:
            terms.append(dict(rollup))
    return terms


def _merge_results(
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    """Combine per-collection heuristic dicts into one. Used when a
    source spans multiple Mongo collections (e.g. ncbi_geo = GSE + GSM)."""
    if not results:
        return {}
    if len(results) == 1:
        return results[0]

    merged: dict[str, Any] = {}

    # collectionSize: sum doc counts per @type (unitText) across collections.
    sizes: dict[str, int] = {}
    for r in results:
        for item in r.get("collectionSize") or []:
            unit = item.get("unitText")
            if unit is None:
                continue
            sizes[unit] = sizes.get(unit, 0) + int(item.get("minValue", 0))
    if sizes:
        merged["collectionSize"] = [
            {"@type": "QuantitativeValue", "minValue": v, "unitText": k}
            for k, v in sizes.items()
        ]

    # dateModified: max across collections (lexicographic works for ISO dates).
    mods = [r["dateModified"] for r in results if r.get("dateModified")]
    if mods:
        merged["dateModified"] = max(mods)

    # temporalCoverage: union of intervals → min startDate, max endDate.
    starts: list[str] = []
    ends: list[str] = []
    for r in results:
        for iv in r.get("temporalCoverage") or []:
            if iv.get("startDate"):
                starts.append(iv["startDate"])
            if iv.get("endDate"):
                ends.append(iv["endDate"])
    if starts or ends:
        interval: dict[str, Any] = {
            "@type": "TemporalInterval",
            "temporalType": "collection",
        }
        if starts:
            interval["startDate"] = min(starts)
        if ends:
            interval["endDate"] = max(ends)
        merged["temporalCoverage"] = [interval]

    # license / usageInfo consensus: agree → value, disagree → "Varies".
    for field in ("license", "usageInfo"):
        values = [r[field] for r in results if field in r]
        if not values:
            continue
        first = values[0]
        if all(v == first for v in values[1:]):
            merged[field] = first
        else:
            merged[field] = "Varies"

    # Union + dedupe for list-of-objects fields.
    for field in DEFINED_TERM_FIELDS + ["spatialCoverage"]:
        seen: set[str] = set()
        combined: list[dict[str, Any]] = []
        for r in results:
            for item in r.get(field) or []:
                sig = json.dumps(item, sort_keys=True, default=str)
                if sig in seen:
                    continue
                seen.add(sig)
                combined.append(item)
        if not combined:
            continue
        # Re-apply the top-N + rollup rule after union so multi-
        # collection sources follow the same cap as single-collection.
        if field in DEFINED_TERM_FIELDS:
            combined = _apply_top_n_and_rollup(combined, field)
        merged[field] = combined

    return merged


def _prefix_match(
    pipeline: list[dict[str, Any]],
    mongo_filter: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Prepend a ``$match`` stage if a filter was provided. Used to
    scope aggregations to a subset of a shared collection (e.g. VEuPath
    children inside ``veupath_collections``)."""
    if not mongo_filter:
        return pipeline
    return [{"$match": mongo_filter}, *pipeline]


def compute_for_source(
    collection,
    mongo_filter: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run all aggregations for one Mongo collection and return the
    heuristic field dict. Never raises on missing fields — each
    aggregation is independent and its failure only drops that field.

    ``mongo_filter`` is prepended as a ``$match`` stage to every
    aggregation, so the heuristics reflect only the matching subset."""
    result: dict[str, Any] = {}

    # collectionSize per @type
    try:
        buckets = list(collection.aggregate(
            _prefix_match(agg_collection_size(), mongo_filter)
        ))
        sizes = [
            {
                "@type": "QuantitativeValue",
                "minValue": b["count"],
                "unitText": b["_id"],
            }
            for b in buckets
            if b.get("_id")
        ]
        if sizes:
            result["collectionSize"] = sizes
    except Exception as e:
        logging.warning("collectionSize failed: %s", e)

    # dates (for dateModified + temporalCoverage)
    max_mod = None
    try:
        rows = list(collection.aggregate(
            _prefix_match(agg_dates(), mongo_filter)
        ))
        if rows:
            r = rows[0]
            max_mod = _normalize_date(r.get("max_dateModified"))
            min_created = _normalize_date(r.get("min_dateCreated"))
            min_pub = _normalize_date(r.get("min_datePublished"))
            start = min(filter(None, [min_created, min_pub]), default=None)
            if max_mod:
                result["dateModified"] = max_mod
            if start or max_mod:
                interval: dict[str, Any] = {
                    "@type": "TemporalInterval",
                    "temporalType": "collection",
                }
                if start:
                    interval["startDate"] = start
                if max_mod:
                    interval["endDate"] = max_mod
                result["temporalCoverage"] = [interval]
    except Exception as e:
        logging.warning("dates failed: %s", e)

    # license / usageInfo consensus
    for field in ("license", "usageInfo"):
        try:
            distinct = list(collection.aggregate(
                _prefix_match(agg_consensus(field), mongo_filter)
            ))
        except Exception as e:
            logging.warning("%s consensus failed: %s", field, e)
            continue
        values = [d["_id"] for d in distinct if d.get("_id") is not None]
        if len(values) == 1:
            result[field] = values[0]
        elif len(values) > 1:
            result[field] = "Varies"

    # spatialCoverage
    try:
        rows = list(collection.aggregate(
            _prefix_match(agg_spatial_coverage(), mongo_filter)
        ))
        places = [r["_id"] for r in rows if r.get("_id")]
        if places:
            result["spatialCoverage"] = [
                {"@type": "AdministrativeArea", "name": p} for p in places
            ]
    except Exception as e:
        logging.warning("spatialCoverage failed: %s", e)

    # DefinedTerm fields: rank by frequency, keep top N, emit rollup
    # when truncated. Richer DefinedTerm shape (identifier/name/url/
    # inDefinedTermSet/termCode) matches records in the source DB.
    for field in DEFINED_TERM_FIELDS:
        try:
            rows = list(collection.aggregate(
                _prefix_match(agg_defined_term(field), mongo_filter)
            ))
        except Exception as e:
            logging.warning("%s aggregation failed: %s", field, e)
            continue
        # Buckets arrive sorted by count desc; dedupe identical
        # (identifier, name) keys defensively.
        terms: list[dict[str, Any]] = []
        seen: set[tuple] = set()
        for row in rows:
            key = row.get("_id") or {}
            ident = key.get("identifier")
            name = key.get("name")
            if not ident and not name:
                continue
            sig = (ident, name)
            if sig in seen:
                continue
            seen.add(sig)
            term: dict[str, Any] = {"@type": "DefinedTerm"}
            if ident:
                term["identifier"] = ident
            if name:
                term["name"] = name
            if row.get("url"):
                term["url"] = row["url"]
            if row.get("inDefinedTermSet"):
                term["inDefinedTermSet"] = row["inDefinedTermSet"]
            if row.get("termCode"):
                term["termCode"] = row["termCode"]
            terms.append(term)
        if not terms:
            continue
        result[field] = _apply_top_n_and_rollup(terms, field)

    return result


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def get_client(url: str):
    """Imported lazily so --dry-run works without pymongo installed."""
    from pymongo import MongoClient
    return MongoClient(url, serverSelectionTimeoutMS=10000)


def load_repos(only: str | None = None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(REPO_METADATA_DIR.glob("*.json")):
        if path.name.startswith("_"):
            continue  # skip the JSON Schema and other internals
        key = path.stem
        if only and key != only:
            continue
        out[key] = json.loads(path.read_text())
    return out


def write_cache(key: str, data: dict[str, Any]) -> Path:
    HEURISTICS_DIR.mkdir(parents=True, exist_ok=True)
    path = HEURISTICS_DIR / f"{key}.json"
    with path.open("w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        f.write("\n")
    return path


def describe_queries(key: str) -> None:
    """Print the aggregation pipelines we'd run for one source."""
    print(f"--- {key} ---")
    print("collectionSize:", json.dumps(agg_collection_size()))
    print("dates:", json.dumps(agg_dates()))
    print("license consensus:", json.dumps(agg_consensus("license")))
    print("usageInfo consensus:", json.dumps(agg_consensus("usageInfo")))
    print("spatialCoverage:", json.dumps(agg_spatial_coverage()))
    for field in DEFINED_TERM_FIELDS:
        print(f"{field}:", json.dumps(agg_defined_term(field)))


def run(url: str, db_name: str, only: str | None, dry_run: bool) -> int:
    repos = load_repos(only=only)
    if not repos:
        print(f"No repos matched (only={only!r})")
        return 1
    if dry_run:
        for key in repos:
            describe_queries(key)
        return 0
    client = get_client(url)
    db = client[db_name]
    available = set(db.list_collection_names())
    written = 0
    skipped = 0
    for key, repo in repos.items():
        # Allow a per-repo override of the Mongo collection name.
        # Accepts a string or a list of collection names — for sources
        # that span multiple collections (e.g. ncbi_geo = gse_ncbi_geo
        # + gsm_ncbi_geo) results are merged across collections.
        override = repo.get("_mongoCollection") or key
        names = [override] if isinstance(override, str) else list(override)
        missing = [n for n in names if n not in available]
        if missing:
            skipped += 1
            logging.info(
                "%s: missing Mongo collection(s) %r in %s, skipping",
                key, missing, db_name,
            )
            continue
        mongo_filter = repo.get("_mongoFilter")
        per_collection = [
            compute_for_source(db[n], mongo_filter=mongo_filter)
            for n in names
        ]
        heuristic = _merge_results(per_collection)
        path = write_cache(key, heuristic)
        written += 1
        rel = path.relative_to(REPO_ROOT)
        suffix_parts = []
        if len(names) > 1:
            suffix_parts.append(f"merged across {names}")
        if mongo_filter:
            suffix_parts.append(f"filter={mongo_filter}")
        suffix = f" ({'; '.join(suffix_parts)})" if suffix_parts else ""
        print(
            f"{key}: wrote {len(heuristic)} heuristic fields to {rel}"
            f"{suffix}"
        )
    print(
        f"Done. Wrote {written}, skipped {skipped}, "
        f"total {len(repos)} sources."
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mongo-url",
        default=DEFAULT_MONGO_URL,
        help=(
            "MongoDB connection URL "
            f"(default: $MONGO_URL or {DEFAULT_MONGO_URL})"
        ),
    )
    parser.add_argument(
        "--mongo-db",
        default=DEFAULT_MONGO_DB,
        help=(
            "Mongo database name "
            f"(default: $MONGO_DB or {DEFAULT_MONGO_DB})"
        ),
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Only compute for this source key. Default: all.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the aggregation pipelines without connecting.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable INFO-level logging.",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(message)s",
    )
    return run(
        url=args.mongo_url,
        db_name=args.mongo_db,
        only=args.source,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
