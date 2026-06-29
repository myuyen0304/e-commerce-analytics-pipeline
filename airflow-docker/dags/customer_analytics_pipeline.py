import os
from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.standard.operators.bash import BashOperator
from cosmos import (
    ProjectConfig,
    ProfileConfig,
    DbtTaskGroup
)

DBT_PROJECT_PATH = "/opt/airflow/dbt_project"

# Airbyte connection UUID (Connections page in the Airbyte UI -> copy the id from the URL).
# Set it via the AIRBYTE_CONNECTION_ID env var (see airflow-docker/.env) so it is not
# hardcoded. Falls back to a placeholder that will fail loudly until configured.
AIRBYTE_CONNECTION_ID = os.environ.get("AIRBYTE_CONNECTION_ID", "REPLACE_WITH_AIRBYTE_CONNECTION_UUID")

with DAG(dag_id='customer_analytics_pipeline',
         schedule='@once',
         max_active_runs=1
    ) as dag:

    trigger_sync= AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync',
        airbyte_conn_id='airbyte_conn',  # connection id from Airflow (configure in UI)
        connection_id=AIRBYTE_CONNECTION_ID,  # connection UUID from Airbyte
        asynchronous=True,
    )

    monitor_sync = AirbyteJobSensor(
        task_id='wait_for_airbyte_sync',
        airbyte_conn_id='airbyte_conn', # connection id from Airflow
        airbyte_job_id= trigger_sync.output
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
    
    trigger_sync >> monitor_sync >> dbt_task >> end_task