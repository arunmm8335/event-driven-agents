"""
producer.py — Publishes fake pipeline failure events to the 'pipeline_failures' Kafka topic.

Run this in one terminal AFTER Kafka is up:
    python producer.py

You can also run it multiple times to flood the agent with events.
Add --loop flag to keep publishing indefinitely.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------------
# Sample failure events — edit or extend freely
# ---------------------------------------------------------------------------

SAMPLE_EVENTS = [
    {
        "job_id": "risk_calc_104",
        "error_log": "java.sql.SQLTimeoutException: Timeout connecting to DB after 30s",
        "source": "risk-engine",
    },
    {
        "job_id": "etl_pipeline_07",
        "error_log": "OOMKilled: container risk-etl exceeded memory limit 2Gi",
        "source": "etl-cluster",
    },
    {
        "job_id": "report_gen_02",
        "error_log": "Connection refused: could not connect to server on port 5432",
        "source": "reporting-service",
    },
    {
        "job_id": "risk_calc_104",
        "error_log": "AttributeError: 'NoneType' object has no attribute 'price'",
        "source": "risk-engine",
    },
    {
        "job_id": "data_ingest_55",
        "error_log": "PermissionError: Access denied to s3://prod-data-lake/raw/pricing",
        "source": "ingest-worker",
    },
    {
        "job_id": "etl_pipeline_07",
        "error_log": "OSError: [Errno 28] No space left on device",
        "source": "etl-cluster",
    },
]

TOPIC = "pipeline_failures"
BOOTSTRAP_SERVERS = "localhost:9092"


# ---------------------------------------------------------------------------
# Producer setup
# ---------------------------------------------------------------------------

def make_producer(retries: int = 5, delay: int = 3) -> KafkaProducer:
    """Create a KafkaProducer, retrying until Kafka is ready."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",                 # wait for broker acknowledgement
                retries=3,
            )
            print(f"[producer] Connected to Kafka at {BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"[producer] Kafka not ready (attempt {attempt}/{retries}), retrying in {delay}s…")
            time.sleep(delay)
    print("[producer] ERROR: Could not connect to Kafka. Is Docker Kafka running?")
    sys.exit(1)


def send_events(loop: bool = False) -> None:
    producer = make_producer()
    events = SAMPLE_EVENTS

    try:
        while True:
            for event in events:
                payload = {**event, "timestamp": datetime.now(timezone.utc).isoformat()}
                future = producer.send(TOPIC, payload)
                future.get(timeout=10)          # block until ack
                print(f"[producer] ✓ Sent  job={payload['job_id']!r:20s}  error={payload['error_log'][:55]!r}")
                time.sleep(1)

            producer.flush()
            print(f"[producer] — batch of {len(events)} events sent —")

            if not loop:
                break

            print("[producer] Looping again in 10s…  (Ctrl-C to stop)")
            time.sleep(10)

    except KeyboardInterrupt:
        print("\n[producer] Interrupted — shutting down.")
    finally:
        producer.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline failure event producer")
    parser.add_argument("--loop", action="store_true", help="Keep sending events in a loop")
    args = parser.parse_args()
    send_events(loop=args.loop)
