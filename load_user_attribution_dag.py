from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

# DAG default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='load_user_attribution_dag',
    default_args=default_args,
    description='Runs the BigQuery routine to load user attribution data',
    schedule_interval='0 2 * * *',  # Runs daily at 2 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['bigquery', 'user_attribution'],
) as dag:

    run_bq_stored_procedure = BigQueryInsertJobOperator(
        task_id='run_bq_procedure',
        configuration={
            "query": {
                "query":"CALL `endless-upgrade-475014-b1.g4a_events.user_attribution_routine`();",
                "useLegacySql": False,
            }
        },
    )

    run_bq_stored_procedure
