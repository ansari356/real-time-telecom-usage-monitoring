"""
Kafka Producer - Telecom Real-Time Usage Events
-------------------------------------------------
Simulates live subscriber usage events (calls, data, sms) and publishes
them to a self-hosted Kafka broker (running on EC2) as JSON messages.

Requires: pip install kafka-python

Set the broker address as an environment variable before running
(PowerShell example):
    setx EC2_KAFKA_BROKER "YOUR_EC2_IP"

Note: setx saves the variable permanently but only takes effect in NEW
terminal windows opened AFTER running it. Close and reopen PowerShell
before running this script. Alternatively, just edit the DEFAULT_BROKER
value below directly.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------
DEFAULT_BROKER = "YOUR_EC2_IP"  # <-- update if your EC2 public IP changes
KAFKA_BROKER = os.environ.get("EC2_KAFKA_BROKER", DEFAULT_BROKER)
TOPIC_NAME = "telecom_usage_events"

# Same subscriber pool used in the batch project (1 to 150)
N_SUBSCRIBERS = 150

# Cell towers to simulate geographic spread
CELL_TOWERS = [f"TOWER_{i:03d}" for i in range(1, 21)]

EVENT_TYPES = ["CALL", "DATA", "SMS"]

# Small chance of intentionally "bad" events, so the Databricks
# validation/dedup step downstream has real problems to catch.
BAD_EVENT_RATE = 0.05
DUPLICATE_RATE = 0.03


def build_event(event_id: str) -> dict:
    subscriber_id = random.randint(1, N_SUBSCRIBERS)
    event_type = random.choice(EVENT_TYPES)

    call_duration_sec = None
    data_used_mb = None
    sms_count = None

    if event_type == "CALL":
        call_duration_sec = round(random.uniform(5, 1800), 1)
    elif event_type == "DATA":
        data_used_mb = round(random.uniform(1, 500), 2)
    else:  # SMS
        sms_count = random.randint(1, 5)

    event = {
        "event_id": event_id,
        "subscriber_id": subscriber_id,
        "event_type": event_type,
        "call_duration_sec": call_duration_sec,
        "data_used_mb": data_used_mb,
        "sms_count": sms_count,
        "cell_tower_id": random.choice(CELL_TOWERS),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Inject occasional bad data on purpose (for the validation step later)
    if random.random() < BAD_EVENT_RATE:
        bad_field = random.choice(["negative_value", "null_subscriber", "future_timestamp"])
        if bad_field == "negative_value":
            if event["data_used_mb"] is not None:
                event["data_used_mb"] = -abs(event["data_used_mb"])
            elif event["call_duration_sec"] is not None:
                event["call_duration_sec"] = -abs(event["call_duration_sec"])
        elif bad_field == "null_subscriber":
            event["subscriber_id"] = None
        elif bad_field == "future_timestamp":
            event["event_timestamp"] = "2030-01-01T00:00:00+00:00"

    return event


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Producing events to topic '{TOPIC_NAME}' on {KAFKA_BROKER} ... Ctrl+C to stop.")

    try:
        while True:
            event_id = str(uuid.uuid4())
            event = build_event(event_id)

            producer.send(TOPIC_NAME, value=event)
            print(event)

            # Occasionally resend the same event to simulate real-world duplicates
            if random.random() < DUPLICATE_RATE:
                producer.send(TOPIC_NAME, value=event)

            # time.sleep(random.uniform(0.2, 0.5))  # a few events per second
            time.sleep(0.01)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()