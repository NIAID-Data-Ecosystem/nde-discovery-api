#!/bin/sh

cd /data/
for idx_name in *
do
  echo "loading ${idx_name} ..."
  python3 /load_es.py \
    --host ${ES_HOST} \
    --index ${idx_name} \
    --mappings /data/${idx_name}/mappings.json \
    --settings /data/${idx_name}/settings.json \
    < /data/${idx_name}/data.ndjson
done