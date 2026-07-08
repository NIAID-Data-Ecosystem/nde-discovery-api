# BioThings based API for searching NDE Data

Only `nde-web` is required. The other container is for loading demo data only.

## Daily Elasticsearch Backups

The production web app writes a daily zipped JSON backup of the
`nde_user_profiles` Elasticsearch index to S3. It uses the normal BioThings
Elasticsearch config and uploads to `s3://nde/es_backup/`. AWS credentials
are resolved through the normal `boto3` credential chain.
