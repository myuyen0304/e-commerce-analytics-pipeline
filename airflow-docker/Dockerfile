FROM apache/airflow:3.2.2

USER airflow

RUN pip install --no-cache-dir \
    astronomer-cosmos \
    dbt-core \
    dbt-databricks \
    "dlt[databricks,sql_database]" \
    pandas \
    psycopg2-binary \
    sqlalchemy
