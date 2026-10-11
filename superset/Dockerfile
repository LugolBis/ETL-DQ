FROM apache/superset:6.1.0
USER root
RUN uv pip install --python /app/.venv/bin/python duckdb-engine
COPY --chmod=755 init.sh /app/init.sh
USER superset