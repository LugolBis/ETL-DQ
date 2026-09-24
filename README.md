# ETL_DQ

## Getting started

### Prerequisites

- Docker & the Docker Compose plugin
- Git
- A Kaggle account with an API token

### Installation

```bash
git clone https://github.com/LugolBis/ETL-DQ.git
cd ETL-DQ
cp .env.example .env
```

### Configure the `.env` file

Edit the `.env` file you just created. It's grouped into four sections; here's what to fill in and, for secrets, how to generate them.

| Variable | Purpose | How to obtain / generate it |
|---|---|---|
| `AIRFLOW_UID` | Host UID that owns the files Airflow writes to mounted volumes (avoids permission issues). | Linux/macOS:<br>`id-u`<br>On Windows, leave the default. |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Signs the JWTs issued by Airflow's internal execution API. | Generate a key :<br>`openssl rand -hex 32`|
| `AIRFLOW__API__SECRET_KEY` | Secret key for the Airflow API server (sessions/CSRF). | `openssl rand -hex 32`|
| `FERNET_KEY` | Used as `AIRFLOW__CORE__FERNET_KEY` to encrypt connections/variables stored in the metadata database. | `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `DOCKER_GID` | GID of the host's `docker` group, added to the `airflow-worker` container so it can reach the Docker socket and drive the dbt-runner container. | Linux:<br>`getent group docker \| cut -d: -f3` |

### Build and run

```bash
docker compose up -d --build
```

This builds the custom image `etl-dq`, starts Postgres and Redis and starts every Airflow service.

Once everything is healthy, open the Airflow UI at **http://localhost:8080** (default credentials `airflow` / `airflow`, unless you overrode `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`), unpause a DAG and trigger it.

> [!WARNING]
> The Python code of Airflow Dags need to be in the folder `src/orchestration/dags/`.