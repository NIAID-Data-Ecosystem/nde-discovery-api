# BioThings based API for searching NDE Data

Only `nde-web` is required. The other container is for loading demo data only.

## Daily Elasticsearch Backups

The production web app writes a daily zipped JSON backup of the
`nde_user_profiles` Elasticsearch index to S3. It uses the normal BioThings
Elasticsearch config and uploads to `s3://nde/es_backup/`. AWS credentials
are resolved through the normal `boto3` credential chain. Retention and storage
class transitions are managed by the S3 bucket lifecycle policy. Use
`backup.restore_from_s3(config)` to restore the latest user-profile backup, or
pass a backup filename/key to restore a specific object. See the [workflow
visual](docs/daily_s3_backup_workflow.md) for the function-by-function flow.
