from google.cloud import bigquery
import pandas as pd
import uuid
import random
import time
from datetime import datetime
import json

client = bigquery.Client()
TABLE_ID = "endless-upgrade-475014-b1.g4a_events.events_raw"

EVENT_NAMES = ["page_view", "session_start", "purchase", "add_to_cart"]
TRAFFIC_SOURCES = ["google", "facebook", "newsletter", "direct", "twitter"]

def generate_random_df(n=5):
    rows = []
    for _ in range(n):
        rows.append({
            "event_id": str(uuid.uuid4()),
            "event_name": random.choice(EVENT_NAMES),
            "event_timestamp": int(time.time() * 1_000_000),
            "user_pseudo_id": str(random.randint(1000, 9999)),
            "traffic_source": random.choice(TRAFFIC_SOURCES),
            "event_params":None,
            "received_at": datetime.utcnow()
        })
    return pd.DataFrame(rows)

def stream_random_events(request):
    try:
        request_json = request.get_json(silent=True)
        n_events = int(request_json.get("n_events", 1)) if request_json else 1
    except Exception:
        n_events = 1

    df = generate_random_df(n_events)

    # Load the dataframe to BigQuery
    job = client.load_table_from_dataframe(
        df,
        TABLE_ID,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()  # Wait for completion

    return {"status": "success", "rows_inserted": len(df)}, 200
