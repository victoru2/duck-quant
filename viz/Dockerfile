FROM apache/superset:4.1.2

USER root

RUN pip install --no-cache-dir duckdb duckdb-engine

COPY superset_exports /app/superset_exports
COPY superset_bootstrap.sh /app/superset_bootstrap.sh

RUN chmod +x /app/superset_bootstrap.sh

USER superset

ENTRYPOINT ["/app/superset_bootstrap.sh"]
