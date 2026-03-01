"""
schemas.py — Dataclass definitions for Kafka event payloads.

Two topics:
  pipeline_failures    → PipelineFailureEvent
  pipeline_diagnostics → DiagnosticResult
"""

from dataclasses import dataclass, field, asdict
from typing import List, Optional
import json


@dataclass
class PipelineFailureEvent:
    job_id: str
    error_log: str
    timestamp: str
    source: str = "unknown"

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class DiagnosticResult:
    job_id: str
    error_type: str
    severity: str
    likely_cause: str
    remediation_steps: List[str]
    agent_summary: str
    diagnosed_at: str
    job_history: Optional[dict] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self))
