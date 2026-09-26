# Stage 1 : Builder
ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION} AS builder

LABEL maintainer="LugolBis"

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh

USER airflow
WORKDIR /opt/airflow

COPY --chown=airflow:root pyproject.toml uv.lock ./

RUN uv sync --no-dev --no-install-project

# Stage 2 : Final image
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

LABEL maintainer="LugolBis"

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    libpq5 \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/airflow

COPY --from=builder --chown=airflow:root /opt/airflow/.venv /opt/airflow/.venv

ENV PATH="/opt/airflow/.venv/bin:$PATH"

COPY --chown=airflow:root src/ /opt/airflow/

ENV PYTHONPATH=/opt/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/orchestration/dags