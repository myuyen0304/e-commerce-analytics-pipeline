"""Load the Olist source tables from Postgres into Databricks using dlt.

Replaces the previous Airbyte-based EL. dlt (`sql_database` source ->
`databricks` destination) streams each table in chunks (sqlalchemy backend,
the default), so memory stays bounded even for the ~1M-row geolocation table.
Data lands in `<catalog>.olist_raw.*`, the same target the dbt `_sources.yml`
points at.

Connection contexts (parameterize, never hardcode):
  * Local run:        OLIST_SOURCE_DSN=postgresql://olist:olist@localhost:5433/olist
  * Airflow worker:   OLIST_SOURCE_DSN=postgresql://olist:olist@source-postgres:5432/olist

Databricks creds are reused from the existing DBT_DATABRICKS_* env vars.

Usage:
  uv run python airflow-docker/scripts/load_olist_to_databricks.py            # all 9 tables
  uv run python airflow-docker/scripts/load_olist_to_databricks.py customers  # subset (for testing)
"""

import os
import sys

import dlt
from dlt.destinations import databricks
from dlt.sources.sql_database import sql_database

SOURCE_DSN = os.environ.get(
    "OLIST_SOURCE_DSN",
    "postgresql://olist:olist@localhost:5433/olist",
)
SOURCE_SCHEMA = os.environ.get("OLIST_SOURCE_SCHEMA", "olist_raw")
DATASET_NAME = os.environ.get("OLIST_DATASET", "olist_raw")

ALL_TABLES = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "category_translation",
    "geolocation",
]


def databricks_destination():
    return databricks(
        credentials={
            "server_hostname": os.environ["DBT_DATABRICKS_HOST"],
            "http_path": os.environ["DBT_DATABRICKS_HTTP_PATH"],
            "access_token": os.environ["DBT_DATABRICKS_TOKEN"],
            "catalog": os.environ["DBT_DATABRICKS_CATALOG"],
        },
    )


def run(tables=None):
    tables = tables or ALL_TABLES
    source = sql_database(SOURCE_DSN, schema=SOURCE_SCHEMA).with_resources(*tables)

    pipeline = dlt.pipeline(
        pipeline_name="olist",
        destination=databricks_destination(),
        dataset_name=DATASET_NAME,
    )

    load_info = pipeline.run(source, write_disposition="replace")
    print(load_info)
    return load_info


if __name__ == "__main__":
    requested = sys.argv[1:] or None
    run(requested)
