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

It prompts you to manually download the private `SourceMetaCuration -
resource_base` tab as TSV from:

```text
https://docs.google.com/spreadsheets/d/1SjZ7BNC6oah722psQ_q8oFDB5ZBZjo3np5lBtA3cN-k/edit#gid=349233573
```

Select the `resource_base` tab, download it as tab-separated values
(`.tsv`, current sheet), then move/rename the file to the root of this
repository as:

```text
SourceMetaCuration - resource_base.tsv
```

After the TSV is in place, press Enter in the script prompt. It then runs:

```bash
./nde-web/venv/bin/python nde-web/scripts/sync_repo_metadata.py --source <source>
./nde-web/venv/bin/python nde-web/scripts/compute_heuristics.py --source <source> --verbose
./nde-web/venv/bin/python nde-web/scripts/metadata_compatibility_calculator.py \
  --datasource <source> \
  --cache-dir nde-web/metadata_completeness
./nde-web/venv/bin/python nde-web/scripts/validate_repo_metadata.py --source <source>
```

For non-interactive defaults after the TSV is already at the repository root:

```bash
./nde-web/venv/bin/python nde-web/scripts/bootstrap_source_metadata.py --source uniprot -y
```

## Bootstrap All Existing Sources

To run the same bootstrap flow for every existing source JSON in
`nde-web/repo_metadata/`, use:

```bash
./nde-web/venv/bin/python nde-web/scripts/bootstrap_source_metadata.py --all -y
```

If you only need to apply the updated TSV fields and validate, without
regenerating Mongo-backed heuristic and completeness caches:

```bash
./nde-web/venv/bin/python nde-web/scripts/bootstrap_source_metadata.py --all -y \
  --skip-heuristics \
  --skip-completeness
```

For a new source that does not yet have `repo_metadata/<source>.json`, the
bootstrap script creates the stub with an empty `schedule` string and empty
`schema` object. Fill out those two fields in the generated source metadata
before committing.

## Refresh Saved Search Totals

After publishing a new data release, refresh the stored result count for each
user's saved searches:

```bash
./nde-web/venv/bin/python nde-web/scripts/update_saved_search_totals.py \
  --metadata-url https://api.data.niaid.nih.gov/v1/metadata
```

Use `--dry-run` first to count without writing profile updates. When
`--metadata-url` is provided, the script records the processed `build_version`
and `build_date` in the user profile index and skips later runs for the same
build unless `--force` is passed. It also derives the sibling `/v1/query` URL
from `--metadata-url` and uses that API response to compute frontend-equivalent
totals, including the frontend's default date range and BioSample visibility
filter. The script reads Elasticsearch settings from `nde-web/config.py` or
`nde-web/config_web.py` by default; override them with `--es-host`,
`--user-index`, or `--data-index` only when needed.

## Delete Inactive User Profiles

User profile documents track `last_active` when a user logs in or uses the
account-data endpoints. Delete profiles inactive for two years with:

```bash
./nde-web/venv/bin/python nde-web/scripts/delete_inactive_user_profiles.py \
  --dry-run
```

Remove `--dry-run` after reviewing the counts. The script reads Elasticsearch
settings from `nde-web/config.py` or `nde-web/config_web.py` by default, skips
system marker documents, and falls back to `updated` then `created` for legacy
profiles that do not yet have `last_active`.

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
nde-web/scripts/delete_inactive_user_profiles.py
nde-web/scripts/update_saved_search_totals.py
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
