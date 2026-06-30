"""Load the Olist Brazilian E-Commerce CSV files into the source Postgres DB.

This simulates the "source OLTP system" that the dlt EL script reads from. Run it once
after `docker compose up source-postgres` (or wrap it in an Airflow task to
mimic a source that keeps receiving new rows).

Usage (from host, with deps installed: pandas, sqlalchemy, psycopg2-binary):
    python load_olist_to_postgres.py --data-dir ./data

Connection is read from env vars (same names as docker-compose / .env):
    SOURCE_POSTGRES_HOST (default: localhost)
    SOURCE_POSTGRES_PORT (default: 5433)
    SOURCE_POSTGRES_USER (default: olist)
    SOURCE_POSTGRES_PASSWORD (default: olist)
    SOURCE_POSTGRES_DB (default: olist)
    SOURCE_SCHEMA (default: olist_raw)
"""
from __future__ import annotations

import argparse
import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text

# Maps the Kaggle CSV file name -> destination table name in Postgres.
CSV_TO_TABLE = {
    "olist_customers_dataset.csv": "customers",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "category_translation",
}

# Columns to parse as timestamps so dlt/dbt see real datetime types.
DATE_COLUMNS = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "order_items": ["shipping_limit_date"],
    "order_reviews": ["review_creation_date", "review_answer_timestamp"],
}


def build_engine():
    user = os.getenv("SOURCE_POSTGRES_USER", "olist")
    pwd = os.getenv("SOURCE_POSTGRES_PASSWORD", "olist")
    host = os.getenv("SOURCE_POSTGRES_HOST", "localhost")
    port = os.getenv("SOURCE_POSTGRES_PORT", "5433")
    db = os.getenv("SOURCE_POSTGRES_DB", "olist")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}")


def load(data_dir: str, schema: str) -> None:
    engine = build_engine()
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

    missing = [f for f in CSV_TO_TABLE if not os.path.exists(os.path.join(data_dir, f))]
    if missing:
        sys.exit(
            f"Missing {len(missing)} CSV(s) in {data_dir!r}:\n  "
            + "\n  ".join(missing)
            + "\n\nDownload the 'Brazilian E-Commerce Public Dataset by Olist' "
            "from Kaggle and unzip the CSVs into that folder."
        )

    for csv_name, table in CSV_TO_TABLE.items():
        path = os.path.join(data_dir, csv_name)
        parse_dates = DATE_COLUMNS.get(table)
        df = pd.read_csv(path, parse_dates=parse_dates)
        df.to_sql(
            table,
            engine,
            schema=schema,
            if_exists="replace",  # full reload; switch to "append" for incremental demos
            index=False,
            # method="multi" binds rows*cols params per INSERT; Postgres caps at 65535,
            # so keep chunksize small enough for the widest table (products ~9 cols).
            chunksize=1_000,
            method="multi",
        )
        print(f"  loaded {len(df):>7,} rows -> {schema}.{table}")

    print(f"\nDone. {len(CSV_TO_TABLE)} tables loaded into schema '{schema}'.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Olist CSVs into source Postgres.")
    parser.add_argument(
        "--data-dir",
        default=os.getenv("OLIST_DATA_DIR", "./data"),
        help="Folder containing the Olist *.csv files (default: ./data)",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("SOURCE_SCHEMA", "olist_raw"),
        help="Destination schema (default: olist_raw)",
    )
    args = parser.parse_args()
    load(args.data_dir, args.schema)


if __name__ == "__main__":
    main()
