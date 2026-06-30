import os
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from cosmos import (
    ProjectConfig,
    ProfileConfig,
    DbtTaskGroup
)

DBT_PROJECT_PATH = "/opt/airflow/dbt_project"

# EL is done by dlt (replaces the old Airbyte sync). The script streams the Olist
# tables from the source Postgres into Databricks (<catalog>.olist_raw.*) in chunks,
# so memory stays bounded. Source/Databricks config comes from env vars (see .env):
#   OLIST_SOURCE_DSN  -> postgresql://olist:olist@source-postgres:5432/olist (worker)
#   DBT_DATABRICKS_*  -> reused for the dlt databricks destination credentials
DLT_LOAD_SCRIPT = "/opt/airflow/scripts/load_olist_to_databricks.py"

with DAG(dag_id='customer_analytics_pipeline',
         schedule='@once',
         max_active_runs=1
    ) as dag:

    dlt_ingest = BashOperator(
        task_id='dlt_ingest_postgres_to_databricks',
        bash_command=f'python {DLT_LOAD_SCRIPT}',
    )

    dbt_task = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=os.path.join(DBT_PROJECT_PATH, "target/manifest.json"),
        ),
        profile_config=ProfileConfig(
            profile_name="dbt_project",
            target_name='dev',
            profiles_yml_filepath="/opt/airflow/.dbt/profiles.yml",
        )
    )

    end_task = BashOperator(
        task_id="end_task",
        bash_command='echo "Executed all Models.."',
    )

    dlt_ingest >> dbt_task >> end_task
