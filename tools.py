"""
tools.py — Mock diagnostic tool functions.

These simulate real infrastructure calls:
  - analyze_error_log   → classifies the error
  - check_job_history   → returns historical run stats
  - suggest_remediation → returns actionable fix steps

The tool definitions (TOOL_DEFINITIONS) are formatted for the
OpenAI-compatible tools API used by Ollama.
"""

import json
from typing import Any


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

def analyze_error_log(log_text: str) -> dict:
    """
    Classify an error log message into a known error type, assign severity,
    and identify the likely cause.
    """
    log_lower = log_text.lower()

    if "timeout" in log_lower or "timed out" in log_lower:
        return {
            "error_type": "timeout",
            "severity": "high",
            "likely_cause": "Database or downstream service overload, or misconfigured timeout thresholds.",
        }
    if "oom" in log_lower or "out of memory" in log_lower or "oomkilled" in log_lower:
        return {
            "error_type": "oom",
            "severity": "critical",
            "likely_cause": "Container memory limit exceeded; job consumes more RAM than allocated.",
        }
    if "connection refused" in log_lower:
        return {
            "error_type": "connection_refused",
            "severity": "high",
            "likely_cause": "Target service is down, misconfigured port, or firewall blocking connection.",
        }
    if "null pointer" in log_lower or "nullpointer" in log_lower or "attributeerror" in log_lower or "nonetype" in log_lower:
        return {
            "error_type": "null_reference",
            "severity": "medium",
            "likely_cause": "Unhandled null/None value in code; likely bad input data or schema change.",
        }
    if "permission" in log_lower or "unauthorized" in log_lower or "403" in log_lower or "access denied" in log_lower:
        return {
            "error_type": "permission_denied",
            "severity": "medium",
            "likely_cause": "Missing IAM role, incorrect credentials, or resource policy misconfiguration.",
        }
    if "disk" in log_lower or "no space left" in log_lower:
        return {
            "error_type": "disk_full",
            "severity": "critical",
            "likely_cause": "Disk or volume is full; pipeline cannot write output.",
        }
    return {
        "error_type": "unknown",
        "severity": "low",
        "likely_cause": "Error pattern not identified; requires manual log inspection.",
    }


def check_job_history(job_id: str) -> dict:
    """
    Return mock historical run statistics for a given pipeline job.
    Simulates what you'd query from a job metadata database.
    """
    history: dict[str, Any] = {
        "risk_calc_104":   {"last_runs": 5,  "failures_last_7d": 3, "avg_duration_min": 12, "last_success": "2026-02-27"},
        "etl_pipeline_07": {"last_runs": 20, "failures_last_7d": 1, "avg_duration_min": 45, "last_success": "2026-02-28"},
        "report_gen_02":   {"last_runs": 10, "failures_last_7d": 0, "avg_duration_min": 8,  "last_success": "2026-03-01"},
        "data_ingest_55":  {"last_runs": 8,  "failures_last_7d": 5, "avg_duration_min": 30, "last_success": "2026-02-20"},
    }
    return history.get(
        job_id,
        {"last_runs": 0, "failures_last_7d": 0, "avg_duration_min": 0, "last_success": "never", "note": "unknown job"},
    )


def suggest_remediation(error_type: str) -> dict:
    """
    Return prioritised remediation steps for a specific error type.
    """
    remediation_map: dict[str, Any] = {
        "timeout": {
            "priority": "high",
            "steps": [
                "Increase DB connection pool size (current default: 10).",
                "Raise query/socket timeout threshold in job config.",
                "Check CloudWatch / Datadog for DB CPU/connection saturation.",
                "Consider adding a circuit breaker or retry with backoff.",
            ],
        },
        "oom": {
            "priority": "critical",
            "steps": [
                "Increase container memory limit in job spec (e.g. 2Gi → 4Gi).",
                "Profile memory usage with a heap dump or memory profiler.",
                "Switch to incremental/streaming processing to reduce peak RAM.",
                "Investigate data volume spike triggering the OOM.",
            ],
        },
        "connection_refused": {
            "priority": "high",
            "steps": [
                "Verify target service is healthy (kubectl get pods / ECS task status).",
                "Check port mapping and service discovery configuration.",
                "Review security group / VPC firewall rules.",
                "Confirm no recent deployment changed the service endpoint.",
            ],
        },
        "null_reference": {
            "priority": "medium",
            "steps": [
                "Add defensive null checks / Optional handling in job code.",
                "Validate incoming data schema before processing.",
                "Review recent code merges that may have changed field names.",
                "Add schema validation step at pipeline entry point.",
            ],
        },
        "permission_denied": {
            "priority": "medium",
            "steps": [
                "Review IAM role attached to the job execution role.",
                "Check resource-based policies (S3 bucket policy, Secrets Manager).",
                "Rotate and re-inject credentials if stale.",
                "Run aws iam simulate-principal-policy to confirm permissions.",
            ],
        },
        "disk_full": {
            "priority": "critical",
            "steps": [
                "Immediately clear old logs / temp files on the affected node.",
                "Expand the EBS volume or PVC storage claim.",
                "Add disk-space monitoring alert (<20% free → page on-call).",
                "Archive old pipeline outputs to S3/cold storage.",
            ],
        },
    }
    default = {
        "priority": "low",
        "steps": [
            "Collect full stack trace from job logs.",
            "Check application-level error tracking (Sentry / Datadog APM).",
            "Escalate to engineering with full context.",
        ],
    }
    return remediation_map.get(error_type, default)


# ---------------------------------------------------------------------------
# OpenAI-compatible tool definitions (used by agent.py)
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "analyze_error_log",
            "description": (
                "Analyze an error log message. Returns the error type, severity level "
                "(low / medium / high / critical), and the likely root cause."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "log_text": {
                        "type": "string",
                        "description": "The raw error log message from the pipeline failure event.",
                    }
                },
                "required": ["log_text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_job_history",
            "description": (
                "Retrieve historical run statistics for a pipeline job: number of recent runs, "
                "failures in the last 7 days, average duration, and date of last success."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "The unique pipeline job identifier (e.g. 'risk_calc_104').",
                    }
                },
                "required": ["job_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "suggest_remediation",
            "description": (
                "Get a prioritised list of remediation steps for a known error type. "
                "Call this AFTER analyze_error_log so you know the error_type."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "error_type": {
                        "type": "string",
                        "description": (
                            "The error type returned by analyze_error_log. "
                            "One of: timeout, oom, connection_refused, null_reference, "
                            "permission_denied, disk_full, unknown."
                        ),
                    }
                },
                "required": ["error_type"],
            },
        },
    },
]

# Registry for dynamic dispatch inside the agent loop
TOOL_REGISTRY = {
    "analyze_error_log": analyze_error_log,
    "check_job_history": check_job_history,
    "suggest_remediation": suggest_remediation,
}
