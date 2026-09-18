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

On **staging**, it prompts you to manually download the private `RepoMetaCuration -
resource_base` tab as TSV from:

```text
https://docs.google.com/spreadsheets/d/12TFTEWZHQECir2fnjUs8-Oc3qcV2VTlCgGq86Dz7wUc/edit#gid=349233573
```

Select the `resource_base` tab, download it as tab-separated values
(`.tsv`, current sheet), then move/rename the file to the root of this
repository as:

```text
RepoMetaCuration - resource_base.tsv
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

The staging sync treats nonblank RepoMetaCuration values as authoritative for
supported descriptive fields, replacing existing names, descriptions, URLs,
identifiers, and other curated values. Empty cells keep existing values.
Source keys (`_id` and filenames), schedules, schema mappings, source types,
parent collections, and Mongo settings are preserved. This change belongs to
`staging`; production's `main` branch retains its SourceMetaCuration workflow.

Rows are matched using names, pipe-separated aliases/identifiers, URLs, and
`sameAs`, so changed repository URLs can still update existing sources.
Ambiguous or unmatched sources are reported and kept unchanged; new sheet
rows are not automatically added as sources. Malformed TSV rows with the wrong
column count are reported and skipped. Re-export or correct those rows before
bootstrapping their sources.

`alternateName` supports pipe-separated values as well as older comma/semicolon
exports. `genre` remains a semicolon-separated array. Other supported fields
retain their existing API types (for example, `identifier` and `collectionType`
remain strings, including any pipe-separated values in the sheet).

Both bootstrap and sync accept `--resource-base-tsv <path>` to read another
download location; relative paths resolve from the repository root. Bootstrap
passes that same file to sync. Downloaded TSVs stay local; commit the regenerated
JSON files used by `NDESourceHandler`. The API loads those files once per
process, so restart the staging API after deploying refreshed JSON files.

When bootstrapping a brand-new source from a `resource_base.tsv` row, the
script uses the first `alternateName` value as the canonical source key by
default:

```bash
./nde-web/venv/bin/python nde-web/scripts/bootstrap_source_metadata.py \
  --source "United States Immunodeficiency Network (USIDNET)" \
  -y
```

That key controls `repo_metadata/<source>.json`, the source `_id`,
`repo_metadata/heuristics/<source>.json`, and
`metadata_completeness/cache_<source>.json`.

Use `--use-source-key` only when the normalized `--source` value should be
used instead.

### Troubleshooting: Missing Heuristics Cache File

If bootstrap fails at validation with output like:

```text
Running: /.../python nde-web/scripts/validate_repo_metadata.py --source <source>
Warnings:
  - <source>: no heuristic cache file
Errors:
  - nde-web/repo_metadata/heuristics/<source>.json: missing generated file
```

one common cause is that the source key does not match the actual Mongo
collection name. In that case, set `_mongoCollection` in
`nde-web/repo_metadata/<source>.json` to the real collection name.

Example (USIDNET):

```json
"_mongoCollection": "usidnet"
```

Then re-run bootstrap for that source.

## Bootstrap All Existing Sources

To run the same bootstrap flow for every existing source JSON in
`nde-web/repo_metadata/`, use:

```bash
./nde-web/venv/bin/python nde-web/scripts/bootstrap_source_metadata.py --all -y
```

If you only need to apply the updated TSV fields and validate, without
regenerating Mongo-backed heuristic and completeness caches:

```bash
./nde-web/venv/bin/python nde-web/scripts/sync_repo_metadata.py
./nde-web/venv/bin/python nde-web/scripts/validate_repo_metadata.py
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
totals, including the frontend's BioSample visibility filter. Date filters are
used only when they are present on the saved search. The script reads
Elasticsearch settings from `nde-web/config.py` or
`nde-web/config_web.py` by default; override them with `--es-host`,
`--user-index`, or `--data-index` only when needed.

## Warm Filter Query Cache

After publishing a new data release, moving the data alias, clearing
Elasticsearch caches, or restarting Elasticsearch nodes, warm the common
filter-sidebar aggregation requests:

```bash
./nde-web/venv/bin/python nde-web/scripts/warm_filter_cache.py \
  --metadata-url https://api-staging.data.niaid.nih.gov/v1/metadata \
  --dry-run
```

Remove `--dry-run` to execute the requests. The default pass warms the broad
browse-all filter aggregation plus every `Specified` and `Unspecified` filter
option for the unscoped and Shared/Dataset scopes, with both no-date and
default-date variants. This targets the slowest HAR requests while avoiding a
full all-scope warmup by default.

Use `--scopes all` for a full portal-scope warmup, `--fields field_a,field_b`
for a narrower pass, and `--exists-syntax canonical` if the portal is sending
top-level `_exists_` / `-_exists_` filters. The default `legacy` exists syntax
matches the current HAR-style field-scoped URLs. The script calls the public
`/v1/query` API instead of Elasticsearch directly so the Elasticsearch request
cache sees the same query body generated for portal traffic.

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
nde-web/scripts/resource_base.py
nde-web/scripts/compute_heuristics.py
nde-web/scripts/delete_inactive_user_profiles.py
nde-web/scripts/update_saved_search_totals.py
nde-web/scripts/warm_filter_cache.py
nde-web/scripts/validate_repo_metadata.py
nde-web/requirements_scripts.txt
```

Do not commit downloaded sheet exports or ad hoc reports unless the branch
explicitly needs a snapshot:

```text
SourceMetaCuration - resource_base.tsv
RepoMetaCuration - resource_base.tsv
Priority repo metadata - *.tsv
repo_metadata_fields - *.tsv
unstandardized_definedterms.tsv
```
