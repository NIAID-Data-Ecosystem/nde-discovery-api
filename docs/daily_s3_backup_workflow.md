# Daily S3 Backup Workflow

This visual explains the daily Elasticsearch backup added in PR #35. The job
backs up only the production user-profile index and uploads a zipped JSON
archive to `s3://nde/es_backup/`.

```mermaid
flowchart TD
    A["Server starts<br/>nde-web/index.py"] --> B["options.parse_command_line()"]
    B --> C["_load_config_module()<br/>loads config or --conf module"]
    C --> D["_schedule_daily_backup(config)"]
    D --> E["aiocron crontab<br/>0 0 * * *"]
    E --> F["Tornado IOLoop triggers daily job"]
    F --> G["_run_backup_thread(config)<br/>starts daemon thread"]
    G --> H["_run_backup_with_lock(config)<br/>adds small random jitter"]
    H --> I{"Can acquire<br/>.es-backup.lock?"}
    I -- "No" --> J["Skip run<br/>another process has the lock"]
    I -- "Yes" --> K["daily_backup_routine(config)"]

    K --> L["build_es_client(config)<br/>uses config.ES_HOST and ES_ARGS"]
    L --> M["backup_user_index(client,<br/>config.ES_USER_INDEX)"]
    M --> N["client.indices.get(index)<br/>settings, mappings, aliases"]
    M --> O["_iter_index_docs()<br/>helpers.scan match_all"]
    O --> P["_backup_doc(hit)<br/>keeps _id, _source, optional _routing"]
    N --> Q["Backup payload"]
    P --> Q

    Q --> R["write_backup_zip(data)<br/>compressed JSON archive"]
    R --> S["upload_to_s3(archive)<br/>bucket nde, prefix es_backup"]
    S --> T["s3://nde/es_backup/<br/>nde_user_profiles_backup_TIMESTAMP.zip"]
    S --> U["S3 lifecycle policy<br/>handles retention and storage class"]
    U --> V["Log result<br/>index, doc_count, bucket, key"]
```

## Function Map

```mermaid
flowchart LR
    subgraph Scheduler["nde-web/index.py"]
        A["_schedule_daily_backup"]
        B["_run_backup_thread"]
        C["_run_backup_with_lock"]
    end

    subgraph Backup["nde-web/backup.py"]
        D["daily_backup_routine"]
        E["build_es_client"]
        F["backup_user_index"]
        G["_iter_index_docs"]
        H["_backup_doc"]
        I["write_backup_zip"]
        J["upload_to_s3"]
        K["read_backup_from_s3"]
        L["restore_user_index"]
        M["restore_from_s3"]
    end

    A -->|"register midnight cron"| B
    B -->|"runs work off IOLoop thread"| C
    C -->|"single-process guard"| D
    D --> E
    D --> F
    F --> G
    G --> H
    D --> I
    D --> J
    M --> K
    M --> L
```

## Archive Shape

Each uploaded `.zip` contains one JSON file. The JSON is keyed by the
Elasticsearch index name so restore tooling can recreate the index metadata and
then replay documents.

```mermaid
flowchart TD
    A["nde_user_profiles_backup_TIMESTAMP.zip"] --> B["nde_user_profiles_backup_TIMESTAMP.json"]
    B --> C["nde_user_profiles"]
    C --> D["aliases"]
    C --> E["mappings"]
    C --> F["settings"]
    C --> G["docs[]"]
    G --> H["_id"]
    G --> I["_source"]
    G --> J["_routing, when present"]
```

## Explanation Script

1. When the API process starts, `index.py` registers a midnight cron job on the
   same Tornado event loop used by the server.
2. When the cron fires, the work moves into a daemon thread so the server loop
   is not blocked.
3. The thread tries to acquire `.es-backup.lock`. If another process already has
   it, this process skips the run.
4. The backup routine builds a synchronous Elasticsearch client from the normal
   app config.
5. The user-profile index metadata and every document are exported into a
   zipped JSON archive.
6. The archive is uploaded to the existing `nde` S3 bucket under `es_backup/`.
7. Retention and storage-class transitions for uploaded backups are handled by
   the S3 bucket lifecycle policy.
8. To restore, `restore_from_s3(config)` downloads the latest backup object by
   default, reads the zipped JSON payload, and bulk-indexes the saved documents
   into the configured user-profile index.
