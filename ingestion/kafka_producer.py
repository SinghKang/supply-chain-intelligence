# ================================================
# SUPPLY CHAIN INTELLIGENCE PLATFORM
# Kafka Producer — Shipment Events Simulator
# Simulates real-time shipment events from a
# logistics system into Kafka topic
# ================================================

import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ── Configuration ────────────────────────────────
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME   = "shipment-events"

# ── Realistic sample data ─────────────────────────
CARRIERS    = ["FedEx", "DHL", "Blue Dart", "DTDC", "Delhivery"]
STATUSES    = ["ORDER_PLACED", "PROCESSING", "SHIPPED", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "DELAYED"]
ORIGINS     = ["Mumbai", "Delhi", "Bengaluru", "Chennai", "Hyderabad"]
DESTINATIONS= ["Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow"]
PRODUCTS    = ["Laptop", "Phone", "Tablet", "Headphones", "Camera", "Keyboard", "Monitor"]

# ── Producer Setup ────────────────────────────────
# Why value_serializer: Kafka sends bytes not strings
# We convert Python dict → JSON string → bytes
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate_shipment_event():
    """
    Generate a realistic fake shipment event.
    Why random delays: simulates real world where
    shipments don't all arrive at the same time.
    """
    order_id    = random.randint(1000, 9999)
    shipped_at  = datetime.now() - timedelta(days=random.randint(0, 5))
    status      = random.choice(STATUSES)

    # If delivered, add delivery timestamp
    delivered_at = None
    if status == "DELIVERED":
        delivered_at = (shipped_at + timedelta(days=random.randint(1, 7))).isoformat()

    return {
        "shipment_id"     : random.randint(10000, 99999),
        "order_id"        : order_id,
        "product"         : random.choice(PRODUCTS),
        "carrier"         : random.choice(CARRIERS),
        "tracking_number" : f"TRK{random.randint(100000, 999999)}",
        "status"          : status,
        "origin"          : random.choice(ORIGINS),
        "destination"     : random.choice(DESTINATIONS),
        "quantity"        : random.randint(1, 10),
        "weight_kg"       : round(random.uniform(0.5, 50.0), 2),
        "shipped_at"      : shipped_at.isoformat(),
        "delivered_at"    : delivered_at,
        "event_timestamp" : datetime.now().isoformat()
    }

def send_events(num_events=100, delay_seconds=1):
    """
    Send shipment events to Kafka topic.
    Why delay: simulates real time streaming,
    not a bulk dump. 1 event per second.
    """
    print(f"Starting shipment event producer...")
    print(f"Sending to topic: {TOPIC_NAME}")
    print(f"Broker: {KAFKA_BROKER}\n")

    for i in range(num_events):
        event = generate_shipment_event()

        # Send to Kafka
        # Why key=order_id: ensures all events for same
        # order go to same partition — maintains order
        producer.send(
            TOPIC_NAME,
            key=str(event["order_id"]).encode("utf-8"),
            value=event
        )

        print(f"[{i+1}/{num_events}] Sent: Order {event['order_id']} | "
              f"{event['product']} | {event['status']} | "
              f"{event['carrier']} → {event['destination']}")

        time.sleep(delay_seconds)

    producer.flush()
    print("\nAll events sent successfully!")

if __name__ == "__main__":
    send_events(num_events=100, delay_seconds=1)