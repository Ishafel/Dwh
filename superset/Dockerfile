ARG SUPERSET_VERSION=6.0.0
FROM apache/superset:${SUPERSET_VERSION}

USER root

RUN uv pip install --python /app/.venv/bin/python --no-cache \
    clickhouse-connect==0.9.2 \
    psycopg2-binary==2.9.11

USER superset
