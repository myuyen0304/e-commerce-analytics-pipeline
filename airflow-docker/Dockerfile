FROM apache/airflow:3.2.2

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-airbyte==5.5.0 \
    astronomer-cosmos \
    dbt-core \
    dbt-databricks \
    pandas \
    psycopg2-binary \
    sqlalchemy
