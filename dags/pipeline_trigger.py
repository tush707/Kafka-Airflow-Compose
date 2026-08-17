from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="retailrocket_pipeline_trigger_poc",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["retailrocket", "poc"],
) as dag:

    trigger_pipeline_update = DatabricksNotebookOperator(
        task_id="trigger_declarative_pipeline",
        databricks_conn_id="databricks_default",
        pipeline_id="your_pipeline_id_here",
        wait_for_termination=True,
    )