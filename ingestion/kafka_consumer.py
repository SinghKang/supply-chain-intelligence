# ================================================
# SUPPLY CHAIN INTELLIGENCE PLATFORM
# Kafka Consumer — Shipment Events to Bronze Layer
# Reads real-time shipment events from Kafka and
# lands them as raw JSON files in MinIO Bronze bucket
# ================================================

import json
import os
from datetime import datetime
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error
import io

# ── Configuration ────────────────────────────────
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME    = "shipment-events"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS   = "admin"
MINIO_SECRET   = "admin123"
BRONZE_BUCKET  = "bronze"
BATCH_SIZE     = 10  # collect 10 events then write to MinIO

# ── MinIO Client Setup ───────────────────────────
# Why: we need a client to talk to MinIO
# just like boto3 is used for AWS S3
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS,
    secret_key=MINIO_SECRET,
    secure=False  # no SSL locally
)

# ── Kafka Consumer Setup ─────────────────────────
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="bronze-consumer-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=10000  # stop after 10 seconds of no new messages
)

def write_batch_to_minio(events, batch_num):
    """
    Write a batch of events to MinIO Bronze bucket.

    Why batch and not one by one:
    Writing one file per event = thousands of tiny files
    which is the small file problem in data engineering.
    Batching reduces file count and improves performance.

    Why JSON format in Bronze:
    Bronze = raw, exactly as received.
    JSON preserves original structure with no transformation.
    """
    # Create filename with timestamp for partitioning
    # Why date partitioning: allows reading only specific
    # dates instead of scanning all files — much faster
    now        = datetime.now()
    date_path  = now.strftime("%Y/%m/%d")
    filename   = f"shipments/{date_path}/batch_{batch_num}_{now.strftime('%H%M%S')}.json"

    # Convert events to JSON bytes
    data       = json.dumps(events, indent=2).encode("utf-8")
    data_bytes = io.BytesIO(data)

    # Upload to MinIO
    minio_client.put_object(
        BRONZE_BUCKET,
        filename,
        data_bytes,
        length=len(data),
        content_type="application/json"
    )

    print(f"✅ Written batch {batch_num} → bronze/{filename} ({len(events)} events)")

def consume_events():
    """
    Continuously consume events from Kafka
    and land them in MinIO Bronze bucket.
    """
    print(f"Starting consumer...")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Writing to: MinIO Bronze bucket\n")

    batch      = []
    batch_num  = 1

    for message in consumer:
        event = message.value
        batch.append(event)

        print(f"Consumed: Order {event['order_id']} | "
              f"{event['product']} | {event['status']}")

        # When batch is full write to MinIO
        if len(batch) >= BATCH_SIZE:
            write_batch_to_minio(batch, batch_num)
            batch     = []
            batch_num += 1

    
    if batch:
        write_batch_to_minio(batch, batch_num)
        print(f"Written final batch of {len(batch)} events")

    print("Consumer finished — no more messages!")        

if __name__ == "__main__":
    consume_events()