# dbt project (Olist analytics on Databricks)

Transforms the raw Olist tables (landed in Databricks by Airbyte) into analytics-ready
Gold models. Full setup is in the local (untracked) `docs/SETUP.md`.

## Layers

```
olist_raw.* (Airbyte)  ->  staging (views)  ->  gold (tables)
```

- **staging/** — 1:1 cleaned models: `stg_orders`, `stg_order_items`, `stg_customers`,
  `stg_products` (English category names joined), `stg_payments`, `stg_reviews`.
  Deduplicated on natural keys using Airbyte's `_airbyte_extracted_at`.
- **gold/** — `revenue_by_month`, `top_products`, `top_customers`.

## Common commands

```bash
dbt debug                 # check the Databricks connection
dbt build                 # run all models + tests
dbt run  --select staging # or gold
dbt test
dbt parse                 # regenerate target/manifest.json (required by Cosmos/Airflow)
```

## Configuration

- Connection: `~/.dbt/profiles.yml` (copy from `profiles.example.yml`), profile
  `dbt_project`, target `dev`. Reads `DBT_DATABRICKS_*` env vars.
- Raw source namespace: edit `database`/`schema` in `models/staging/_sources.yml` to match
  the Airbyte destination (default catalog `olist`, schema `olist_raw`).

> Revenue excludes orders with status `canceled`/`unavailable`. Customers are deduplicated
> to the real person via `customer_unique_id`.
