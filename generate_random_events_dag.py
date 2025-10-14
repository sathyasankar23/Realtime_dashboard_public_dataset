from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import pandas as pd
import uuid
import random
import time
from datetime import datetime, timezone, timedelta
import logging

# --- Configuration ---
PROJECT_ID = "endless-upgrade-475014-b1"
DATASET_ID = "g4a_events"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.events_raw"

EVENT_NAMES = ["page_view", "session_start", "purchase", "add_to_cart"]
TRAFFIC_SOURCES = [
    {"source": "google", "medium": "cpc", "name": "campaign_google"},
    {"source": "facebook", "medium": "social", "name": "campaign_fb"},
    {"source": "newsletter", "medium": "email", "name": "campaign_news"},
    {"source": "direct", "medium": "none", "name": "campaign_direct"},
    {"source": "twitter", "medium": "social", "name": "campaign_twitter"},
]

SCHEMA = [
    #{"name": "event_date", "mode": "NULLABLE", "type": "DATE"},
    {"name": "user_pseudo_id", "mode": "NULLABLE", "type": "STRING"},
    {"name": "event_name", "mode": "NULLABLE", "type": "STRING"},
    {"name": "traffic_source_name", "mode": "NULLABLE", "type": "STRING"},
    {"name": "traffic_medium", "mode": "NULLABLE", "type": "STRING"},
    {"name": "traffic_campaign", "mode": "NULLABLE", "type": "STRING"},
    #{"name": "event_time", "mode": "NULLABLE", "type": "TIMESTAMP"},
]

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- DAG Definition ---
with DAG(
    dag_id="generate_random_events_dag",
    default_args=DEFAULT_ARGS,
    description="Generate random events and insert into BigQuery",
    schedule_interval="@hourly",  # Run every hour
    start_date=datetime(2025, 10, 14),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:

    def generate_random_df(n=5):
        """Generate a DataFrame of random events."""
        rows = []
        for _ in range(n):
            traffic = random.choice(TRAFFIC_SOURCES)
            event_ts_micro = int(time.time() * 1_000_000)
            rows.append({
                #"event_date": datetime.utcnow().date(),
                "user_pseudo_id": str(random.uniform(1000000.0000, 9999999.0000)),
                "event_name": random.choice(EVENT_NAMES),
                "traffic_source_name": traffic["source"],
                "traffic_medium": traffic["medium"],
                "traffic_campaign": traffic["name"],
                #"event_time": datetime.fromtimestamp(event_ts_micro / 1_000_000, tz=timezone.utc),
            })
        df = pd.DataFrame(rows)
        logging.info(f"Generated DataFrame:\n{df}")
        return df

    def insert_into_bq(**kwargs):
        """Python callable to insert random events into BigQuery."""
        try:
            n_events = kwargs.get("n_events", 5)
            df = generate_random_df(n_events)
            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig(
                schema=SCHEMA,
                create_disposition="CREATE_NEVER",
                write_disposition="WRITE_APPEND",
                autodetect=False,
            )
            job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
            logging.info(f"BigQuery job {job.job_id} started...")
            job.result()
            logging.info(f"Successfully inserted {len(df)} rows into {TABLE_ID}")
        except Exception as e:
            logging.exception(f"Failed to insert rows into BigQuery: {e}")
            raise

    # --- Task ---
    insert_events_task = PythonOperator(
        task_id="insert_random_events",
        python_callable=insert_into_bq,
        op_kwargs={"n_events": 10},  # Number of events per run
    )

    insert_events_task
