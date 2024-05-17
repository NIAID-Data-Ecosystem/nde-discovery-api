FROM python:3.8 AS builder

WORKDIR /build/nde
ADD requirements_web.txt .
WORKDIR /build/wheels
RUN pip wheel -r /build/nde/requirements_web.txt


FROM python:3.8-slim
RUN apt update -q -y && apt install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

RUN useradd -m nde
USER nde
RUN python -m venv /home/nde/venv
COPY --from=builder --chown=nde:nde /build/wheels /home/nde/whl
RUN /home/nde/venv/bin/pip install /home/nde/whl/*.whl && rm -rf /home/nde/whl

WORKDIR /home/nde/nde_backend
ADD config_web.py index.py pipeline.py handlers.py resource_catalogs.json ./

STOPSIGNAL SIGINT

EXPOSE 9000
ENV ES_HOST http://host.docker.internal:9200
ENTRYPOINT ["/home/nde/venv/bin/python", "index.py"]
CMD ["--conf=config_web", "--port=9000"]
