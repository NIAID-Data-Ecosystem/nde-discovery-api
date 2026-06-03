# Repo Metadata Scripts

These scripts keep source-level metadata out of `handlers.py` and split it
into small, reviewable files:

- `nde-web/repo_metadata/<source>.json`: curated source facts and schema mapping
- `nde-web/repo_metadata/heuristics/<source>.json`: generated rollups from Mongo
- `nde-web/metadata_completeness/cache_<source>.json`: generated coverage metrics

## Setup

Use the web venv if it exists, then install script-only dependencies:

```bash
./nde-web/venv/bin/pip install -r nde-web/requirements_scripts.txt
```

## Bootstrap One Source

The main new-source command is:

```bash
./nde-web/venv/bin/python nde-web/scripts/bootstrap_source_metadata.py --source uniprot
```

It prompts before downloading the latest `SourceMetaCuration - resource_base`
tab from Google Sheets as `SourceMetaCuration - resource_base.tsv`, then runs:

```bash
./nde-web/venv/bin/python nde-web/scripts/sync_repo_metadata.py --source <source>
./nde-web/venv/bin/python nde-web/scripts/compute_heuristics.py --source <source> --verbose
./nde-web/venv/bin/python nde-web/scripts/metadata_compatibility_calculator.py \
  --datasource <source> \
  --cache-dir nde-web/metadata_completeness
./nde-web/venv/bin/python nde-web/scripts/validate_repo_metadata.py --source <source>
```

For non-interactive defaults:

```bash
./nde-web/venv/bin/python nde-web/scripts/bootstrap_source_metadata.py --source uniprot -y
```

For a new source that does not yet have `repo_metadata/<source>.json`, pass a
schema mapping JSON if you have one:

```bash
./nde-web/venv/bin/python nde-web/scripts/bootstrap_source_metadata.py \
  --source uniprot \
  --schema-json path/to/schema.json \
  --schedule Weekly
```

If the Google Sheet download needs browser auth, download the tab manually as
TSV and save it at the repo root as `SourceMetaCuration - resource_base.tsv`,
then rerun with `--skip-download`.

## Commit Checklist

Commit source-specific metadata outputs:

```text
nde-web/repo_metadata/<source>.json
nde-web/repo_metadata/heuristics/<source>.json
nde-web/metadata_completeness/cache_<source>.json
```

Commit script changes when they are part of the branch:

```text
nde-web/scripts/bootstrap_source_metadata.py
nde-web/scripts/metadata_compatibility_calculator.py
nde-web/scripts/sync_repo_metadata.py
nde-web/scripts/compute_heuristics.py
nde-web/scripts/validate_repo_metadata.py
nde-web/requirements_scripts.txt
```

Do not commit downloaded sheet exports or ad hoc reports unless the branch
explicitly needs a snapshot:

```text
SourceMetaCuration - resource_base.tsv
Priority repo metadata - *.tsv
repo_metadata_fields - *.tsv
unstandardized_definedterms.tsv
```
