import boto3
import pandas as pd
from io import BytesIO
from kafka import KafkaProducer
import json
import time
import os

# --- Config ---
R2_ENDPOINT = "https://5c41792aa0e026df91d7c43f85e6f82b.r2.cloudflarestorage.com"
R2_ACCESS_KEY = "a738f997bf3611972cdfe1257ec5ffcd"
R2_SECRET_KEY = "a4c6aed7e821f4d2d1cadba977e876278f9a538d0bfaca99c8e76201aa8761d2"
BUCKET = "projectcsv"
KEY = "E-COMMERCE/events.csv"

BATCH_SIZE = 10
PROGRESS_FILE = "producer_progress.txt"   # tracks how many rows sent so far

# --- Step 1: Download events.csv from R2 ---
s3 = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
)

print("Downloading events.csv from R2...")
obj = s3.get_object(Bucket=BUCKET, Key=KEY)
df = pd.read_csv(BytesIO(obj['Body'].read())).sort_values('timestamp').reset_index(drop=True)

# --- Step 2: Figure out where we left off ---
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, "r") as f:
        last_sent_index = int(f.read().strip())
else:
    last_sent_index = 0

start_idx = last_sent_index
end_idx = min(last_sent_index + BATCH_SIZE, len(df))

if start_idx >= len(df):
    print("All rows have already been sent. Nothing left to produce.")
    exit()

df_batch = df.iloc[start_idx:end_idx]
print(f"Sending rows {start_idx} to {end_idx - 1} ({len(df_batch)} events)...")

# --- Step 3: Set up Kafka producer ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3,
    linger_ms=10
)

TOPIC = 'retailrocket-events'
SPEED_FACTOR = 100000

# --- Step 4: Send this batch ---
prev_ts = None
sent_count = 0

for _, row in df_batch.iterrows():
    current_ts = row['timestamp']

    if prev_ts is not None:
        real_gap_sec = (current_ts - prev_ts) / 1000
        sleep_time = real_gap_sec / SPEED_FACTOR
        if sleep_time > 0:
            time.sleep(sleep_time)

    event = {
        "visitorid": int(row['visitorid']),
        "itemid": int(row['itemid']) if pd.notna(row['itemid']) else None,
        "event": row['event'],
        "transactionid": int(row['transactionid']) if pd.notna(row['transactionid']) else None,
        "timestamp": int(current_ts)
    }

    producer.send(TOPIC, key=str(event['visitorid']), value=event)
    sent_count += 1
    prev_ts = current_ts

producer.flush()
producer.close()

# --- Step 5: Save progress for next run ---
with open(PROGRESS_FILE, "w") as f:
    f.write(str(end_idx))

print(f"Done. Sent {sent_count} events (rows {start_idx}-{end_idx - 1}).")
print(f"Next run will start from row {end_idx}.")