"""
consumer_agent.py — Consumes 'pipeline_failures' events, runs the LLM diagnostic
agent on each, and publishes results to 'pipeline_diagnostics'.

Run in a second terminal (after Kafka is up and Ollama is running):
    python consumer_agent.py

This process runs continuously — Ctrl-C to stop.
"""

import json
import logging
import sys
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from agent import run_agent

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "pipeline_failures"
OUTPUT_TOPIC = "pipeline_diagnostics"
CONSUMER_GROUP = "diagnostics-agent-group"


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def make_consumer(retries: int = 5, delay: int = 3) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=CONSUMER_GROUP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=-1,        # block indefinitely (no timeout)
            )
            logger.info("Consumer connected — listening on topic '%s'", INPUT_TOPIC)
            return consumer
        except NoBrokersAvailable:
            logger.warning("Kafka not ready (attempt %d/%d), retrying in %ds…", attempt, retries, delay)
            time.sleep(delay)
    logger.error("Could not connect to Kafka. Is Docker Kafka running?")
    sys.exit(1)


def make_producer(retries: int = 5, delay: int = 3) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info("Producer connected — will publish to topic '%s'", OUTPUT_TOPIC)
            return producer
        except NoBrokersAvailable:
            logger.warning("Kafka not ready (attempt %d/%d), retrying in %ds…", attempt, retries, delay)
            time.sleep(delay)
    logger.error("Could not connect to Kafka.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run() -> None:
    consumer = make_consumer()
    producer = make_producer()

    print()
    print("=" * 65)
    print("  Pipeline Diagnostic Agent — waiting for failure events…")
    print("  Input  topic: pipeline_failures")
    print("  Output topic: pipeline_diagnostics")
    print("  Ctrl-C to stop.")
    print("=" * 65)
    print()

    try:
        for message in consumer:
            event: dict = message.value
            job_id = event.get("job_id", "?")
            error_log = event.get("error_log", "")

            print(f"\n{'─' * 55}")
            logger.info("Received failure event  job=%s", job_id)
            logger.info("Error log: %s", error_log[:100])
            print("  Running LLM agent… (tool calls below)")

            try:
                result = run_agent(event)
            except Exception as exc:
                logger.error("Agent failed for job=%s: %s", job_id, exc, exc_info=True)
                result = {
                    "job_id": job_id,
                    "error_type": "agent_exception",
                    "severity": "unknown",
                    "likely_cause": str(exc),
                    "remediation_steps": [],
                    "agent_summary": f"Agent raised an exception: {exc}",
                    "job_history": {},
                    "diagnosed_at": "",
                }

            future = producer.send(OUTPUT_TOPIC, result)
            future.get(timeout=10)
            logger.info("Diagnosis published → topic=%s  job=%s", OUTPUT_TOPIC, job_id)

    except KeyboardInterrupt:
        print("\n[agent-consumer] Interrupted — shutting down.")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    run()
