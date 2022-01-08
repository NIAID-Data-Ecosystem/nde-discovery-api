FROM python:3.10-slim

ADD requirements.txt /
RUN pip install -r /requirements.txt
ADD load.sh load_es.py wait-for-it.sh /
ADD zenodo_current /data/zenodo_current

ENV ES_HOST=elasticsearch

CMD /wait-for-it.sh --host=${ES_HOST} --port=9200 --timeout=300 -- /load.sh
