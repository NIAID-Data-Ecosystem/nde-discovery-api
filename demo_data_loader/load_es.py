import argparse
import logging
import sys

from elasticsearch import Elasticsearch
import orjson

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--host', type=str, required=False, default="127.0.0.1:9200"
    )
    parser.add_argument(
        '--index', '-i', required=True, type=str
    )
    parser.add_argument(
        '--mappings', '-m', type=str, required=False, default=None
    )
    parser.add_argument(
        '--settings', '-s', type=str, required=False, default=None
    )
    args = parser.parse_args()
    if args.mappings:
        mappings = orjson.loads(open(args.mappings, 'rb').read())
    else:
        mappings = {}
    if args.settings:
        settings = orjson.loads(open(args.settings, 'rb').read())
    else:
        settings = {}
    client = Elasticsearch(args.host)
    if client.indices.exists(index=args.index):
        logging.info("Index %s already exists, ignoring")
        sys.exit(0)
    client.indices.create(index=args.index, body={
        'settings': settings,
        'mappings': {
            'properties': mappings,
        },
    })

    while line := sys.stdin.readline():
        try:
            d = orjson.loads(line)
            doc_id = d.pop('_id')
            client.index(index=args.index, document=d, id=doc_id)
        except Exception as e:
            logging.warning(f"Got Exception {e}")
