# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An end-to-end ELT pipeline demonstrating the Modern Data Stack. Data flows:

```
PostgreSQL --(Airbyte)--> Databricks Delta Lake --(dbt Core)--> Gold analytics models
```

Apache Airflow (with Astronomer Cosmos) orchestrates the whole thing. The Gold-layer data
products are: `customer_kpi`, `customer_category`, `revenue_by_location`, `top_customers`.

Only the **orchestration layer** lives in this repo. Airbyte and Databricks are external
services, and the dbt project is a separate repo (see below).

## Repository layout

- `airflow-docker/` — the only code that runs here. Dockerized Airflow (CeleryExecutor +
  Redis + Postgres) plus the DAG.
  - `dags/customer_analytics_pipeline.py` — the single DAG.
  - `docker-compose.yaml` — Airflow 3.2.2 compose with CeleryExecutor.
  - `Dockerfile` / `requirements.txt` — extends the Airflow image with `astronomer-cosmos`,
    `dbt-core`, `dbt-databricks`, and `apache-airflow-providers-airbyte`.
  - `scripts/load_olist_to_postgres.py` — seeds the Olist dataset into `source-postgres`.
  - `.env.example` — template for all required env vars; copy to `.env` and fill in.
- `dbt_project` — a **git submodule** (gitlink), not present after a plain clone. The actual
  dbt models, `staging/`, and `gold/` directories live there, not in this repo.
- `.dbt/` — gitignored; must exist at repo root containing `profiles.yml` (see below).
- `pyproject.toml` / `uv.lock` — local Python env (managed by `uv`) for running
  `load_olist_to_postgres.py` and optionally dbt outside of Docker.

## The DAG

`customer_analytics_pipeline` runs `@once` and is a linear chain:

```
trigger_airbyte_sync --> wait_for_airbyte_sync --> dbt_models (Cosmos DbtTaskGroup) --> end_task
```

- Airbyte sync is triggered asynchronously, then polled by `AirbyteJobSensor`.
- `AIRBYTE_CONNECTION_ID` env var sets the Airbyte connection UUID; falls back to a
  placeholder that will fail loudly until configured.
- Cosmos renders the dbt project into an Airflow TaskGroup from a **pre-built manifest**
  (`dbt_project/target/manifest.json`) — the manifest must exist for the DAG to parse, so
  run `dbt compile`/`dbt parse` in the submodule before starting the stack.

## Airflow 3.x service architecture

Airflow 3.x splits responsibilities across more containers than earlier versions:

| Service              | Role                                         |
|----------------------|----------------------------------------------|
| `airflow-apiserver`  | REST API + Web UI on port 8080               |
| `airflow-scheduler`  | Schedules DAG runs                           |
| `airflow-dag-processor` | Parses DAG files (separate in Airflow 3.x) |
| `airflow-worker`     | Executes tasks (CeleryExecutor)              |
| `airflow-triggerer`  | Manages deferred tasks                       |
| `postgres`           | Airflow metadata DB                          |
| `source-postgres`    | Olist OLTP source DB (port 5433 on host)     |
| `redis`              | Celery broker                                |

## Volume mounts (already parameterized)

The dbt volumes use env var fallbacks pointing at the repo root:

```yaml
- ${DBT_PROJECT_DIR:-../dbt_project}:/opt/airflow/dbt_project
- ${DBT_PROFILES_DIR:-../.dbt}:/opt/airflow/.dbt
```

The defaults work as-is when running from `airflow-docker/` — they map to the
`dbt_project` submodule and a `.dbt/` folder at the repo root. You only need to
override `DBT_PROJECT_DIR` or `DBT_PROFILES_DIR` in `.env` if your layout differs.

## First-time bootstrap sequence

All docker commands run from `airflow-docker/`.

```bash
# 1. Create .env from template and fill in all values
cp .env.example .env
# Edit .env: generate FERNET_KEY and AIRFLOW__API_AUTH__JWT_SECRET,
# set AIRBYTE_CONNECTION_ID, DBT_DATABRICKS_* credentials.

# 2. Init the dbt submodule and generate the manifest
git submodule update --init --recursive
cd ../dbt_project
dbt deps
dbt compile          # writes target/manifest.json (required for Cosmos)
cd ../airflow-docker

# 3. Create .dbt/profiles.yml at the repo root
#    (see dbt_project/profiles.example.yml if it exists)
mkdir -p ../.dbt
# write profiles.yml there with the 'dbt_project' profile and 'dev' target

# 4. Build and start the stack
docker compose build
docker compose up airflow-init   # one-time DB migrate + create admin user
docker compose up -d

# 5. Load Olist source data (download CSVs from Kaggle first)
#    Run from the repo root with the local venv active (uv run or activate .venv)
uv run python airflow-docker/scripts/load_olist_to_postgres.py \
  --data-dir ./data/olist

# 6. In the Airflow UI, configure the 'airbyte_conn' connection
#    pointing at your Airbyte instance.
```

## Ongoing operations

```bash
# Start/stop (from airflow-docker/)
docker compose up -d
docker compose down          # stop without wiping data
docker compose down -v       # stop and wipe all volumes (full reset)

# Optional service profiles
docker compose --profile flower up    # Celery Flower on :5555
docker compose --profile debug up     # airflow-cli container

# Tail logs for a specific service
docker compose logs -f airflow-scheduler
```

- Airflow UI: http://localhost:8080 (login: `airflow` / `airflow` by default)

## Required `.env` variables

| Variable | Description |
|---|---|
| `FERNET_KEY` | `openssl rand -base64 32` |
| `AIRFLOW__API_AUTH__JWT_SECRET` | `openssl rand -hex 32` (Airflow 3.x required) |
| `AIRFLOW_UID` | `50000` (or `id -u` on Linux) |
| `AIRBYTE_CONNECTION_ID` | UUID from the Airbyte Connections page URL |
| `DBT_DATABRICKS_HOST` | Databricks workspace host |
| `DBT_DATABRICKS_HTTP_PATH` | SQL warehouse HTTP path |
| `DBT_DATABRICKS_TOKEN` | Databricks personal access token |
| `DBT_DATABRICKS_CATALOG` | Databricks catalog (e.g. `olist`) |
| `DBT_DATABRICKS_SCHEMA` | Target schema (e.g. `analytics`) |
| `SOURCE_POSTGRES_*` | Source DB creds (defaults to `olist`/`olist`/`olist` on port 5433) |

`_PIP_ADDITIONAL_REQUIREMENTS` installs packages at container start (quick-dev only);
prefer rebuilding the image via `requirements.txt` / `Dockerfile` for anything lasting.

## Airflow connections (configure in the UI)

- `airbyte_conn` — points at the Airbyte instance (host + port).
- A Databricks target named `dev` under profile `dbt_project` in the mounted
  `/opt/airflow/.dbt/profiles.yml` — env vars in the profile should match the
  `DBT_DATABRICKS_*` values from `.env`.

## Local Python env (uv)

```bash
uv sync              # install deps from pyproject.toml into .venv/
uv sync --group dbt  # add dbt-core + dbt-databricks for local dbt runs
uv run python airflow-docker/scripts/load_olist_to_postgres.py --help
```

## Notes

- There are no automated tests or linters configured in this repo. dbt tests live
  in the `dbt_project` submodule and run via `dbt test` there.
- `README.md` and `Readme.md` are duplicate copies of the same project overview.
- Olist CSVs (~120 MB) are downloaded manually from Kaggle and stored in `data/`
  (gitignored). The load script replaces tables on each run (`if_exists="replace"`);
  switch to `"append"` in the script to demo incremental ingestion.
