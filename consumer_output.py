"""
consumer_output.py — Pretty-prints diagnostic results from 'pipeline_diagnostics'.

Run in a third terminal while consumer_agent.py is running:
    python consumer_output.py

This is your "dashboard" — it continuously shows every diagnosis as it arrives.
"""

import json
import sys
import time
import textwrap

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BOOTSTRAP_SERVERS = "localhost:9092"
OUTPUT_TOPIC = "pipeline_diagnostics"
CONSUMER_GROUP = "diagnostics-output-group"

# ANSI colours (degrades gracefully if terminal doesn't support them)
RED     = "\033[91m"
YELLOW  = "\033[93m"
GREEN   = "\033[92m"
CYAN    = "\033[96m"
BOLD    = "\033[1m"
RESET   = "\033[0m"

SEVERITY_COLOUR = {
    "critical": RED,
    "high":     YELLOW,
    "medium":   CYAN,
    "low":      GREEN,
    "unknown":  RESET,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def colour_severity(severity: str) -> str:
    col = SEVERITY_COLOUR.get(severity.lower(), RESET)
    return f"{col}{severity.upper()}{RESET}"


def make_consumer(retries: int = 5, delay: int = 3) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                OUTPUT_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=CONSUMER_GROUP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=-1,
            )
            return consumer
        except NoBrokersAvailable:
            print(f"[output] Kafka not ready (attempt {attempt}/{retries}), retrying in {delay}s…")
            time.sleep(delay)
    print("[output] ERROR: Could not connect to Kafka.")
    sys.exit(1)


def print_result(result: dict) -> None:
    w = 65  # box width
    sep = "═" * w

    job_id    = result.get("job_id", "unknown")
    err_type  = result.get("error_type", "unknown")
    severity  = result.get("severity", "unknown")
    cause     = result.get("likely_cause", "—")
    steps     = result.get("remediation_steps", [])
    summary   = result.get("agent_summary", "")
    history   = result.get("job_history", {})
    timestamp = result.get("diagnosed_at", "")

    print(f"\n{BOLD}{sep}{RESET}")
    print(f"  {BOLD}DIAGNOSTIC REPORT{RESET}")
    print(sep)
    print(f"  Job ID        : {BOLD}{job_id}{RESET}")
    print(f"  Error Type    : {err_type}")
    print(f"  Severity      : {colour_severity(severity)}")
    print(f"  Likely Cause  : {cause}")
    print(f"  Diagnosed At  : {timestamp}")

    if history:
        runs    = history.get("last_runs", "?")
        fails   = history.get("failures_last_7d", "?")
        dur     = history.get("avg_duration_min", "?")
        last_ok = history.get("last_success", "?")
        print(f"\n  {BOLD}Job History{RESET}")
        print(f"    Last runs       : {runs}")
        print(f"    Failures (7 d)  : {fails}")
        print(f"    Avg duration    : {dur} min")
        print(f"    Last success    : {last_ok}")

    if steps:
        print(f"\n  {BOLD}Remediation Steps{RESET}")
        for i, step in enumerate(steps, 1):
            wrapped = textwrap.fill(step, width=w - 8, subsequent_indent=" " * 8)
            print(f"    {i}. {wrapped}")

    if summary:
        print(f"\n  {BOLD}Agent Summary{RESET}")
        for line in textwrap.wrap(summary, width=w - 4):
            print(f"    {line}")

    print(f"{BOLD}{sep}{RESET}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    consumer = make_consumer()

    print()
    print("=" * 65)
    print("  Diagnostic Output Consumer — waiting for results…")
    print(f"  Listening on topic: {OUTPUT_TOPIC}")
    print("  Ctrl-C to stop.")
    print("=" * 65)

    try:
        for message in consumer:
            result = message.value
            print_result(result)
    except KeyboardInterrupt:
        print("\n[output] Interrupted — shutting down.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
