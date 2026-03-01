"""
agent.py — LLM tool-calling diagnostic agent powered by Ollama.

Uses Ollama's OpenAI-compatible endpoint (http://localhost:11434/v1) so we can
reuse the standard openai Python SDK without any custom HTTP calls.

Recommended models (pull one before running):
  ollama pull llama3.2        ← fast, good tool use (default)
  ollama pull llama3.1        ← larger, better reasoning
  ollama pull qwen2.5:7b      ← excellent tool use

Agent loop logic:
  1. Receive a PipelineFailureEvent dict.
  2. Send system prompt + user message to model with tools attached.
  3. If model responds with tool_calls → execute each tool, append results.
  4. Loop until model responds with finish_reason == "stop".
  5. Build and return a structured DiagnosticResult.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

from openai import OpenAI

from tools import TOOL_DEFINITIONS, TOOL_REGISTRY

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OLLAMA_BASE_URL = "http://localhost:11434/v1"
MODEL = "llama3.2"          # change to "llama3.1" or "qwen2.5:7b" if preferred
MAX_ITERATIONS = 10          # safety cap on the agent loop

logger = logging.getLogger(__name__)

# The openai SDK accepts any string as api_key when pointing at a local server
client = OpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are an expert pipeline diagnostics agent for a financial data platform.

When given a pipeline failure event you MUST:
1. Call `analyze_error_log` with the error log text to classify the error.
2. Call `check_job_history` with the job_id to review recent run history.
3. Call `suggest_remediation` with the error_type from step 1 to get fix steps.

After using all three tools, write a concise diagnostic summary (3-5 sentences)
that explains: what went wrong, how severe it is, whether this is a recurring
issue (based on history), and the top recommended action.

Be direct and actionable. Avoid generic advice — use the tool results.
"""

# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------

def run_agent(event: dict[str, Any]) -> dict[str, Any]:
    """
    Run the diagnostic agent for a single pipeline failure event.

    Args:
        event: dict with at minimum {"job_id": str, "error_log": str}

    Returns:
        dict matching the DiagnosticResult schema fields.
    """
    job_id: str = event.get("job_id", "unknown")
    error_log: str = event.get("error_log", "")

    messages: list[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"Pipeline job '{job_id}' has failed.\n"
                f"Error log: {error_log}\n\n"
                "Please diagnose this failure."
            ),
        },
    ]

    collected_tool_results: dict[str, Any] = {}
    iteration = 0

    logger.info("[agent] Starting diagnosis for job=%s", job_id)

    while iteration < MAX_ITERATIONS:
        iteration += 1
        logger.debug("[agent] Iteration %d — calling model", iteration)

        response = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            tools=TOOL_DEFINITIONS,
            tool_choice="auto",
        )

        choice = response.choices[0]
        finish_reason = choice.finish_reason

        # ── Model wants to call one or more tools ────────────────────────
        if finish_reason == "tool_calls":
            assistant_msg = choice.message
            # Append the full assistant message (with tool_calls) to history
            messages.append(assistant_msg)

            for tool_call in assistant_msg.tool_calls:
                fn_name: str = tool_call.function.name
                fn_args: dict = json.loads(tool_call.function.arguments or "{}")

                print(f"    ↳ [tool] {fn_name}({json.dumps(fn_args)})")
                logger.debug("[agent] Tool call: %s(%s)", fn_name, fn_args)

                fn = TOOL_REGISTRY.get(fn_name)
                if fn is not None:
                    try:
                        result = fn(**fn_args)
                    except Exception as exc:
                        result = {"error": str(exc)}
                else:
                    result = {"error": f"Unknown tool: {fn_name}"}

                collected_tool_results[fn_name] = result
                logger.debug("[agent] Tool result: %s", result)

                # Append the tool result so the model can see it next turn
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": json.dumps(result),
                    }
                )

        # ── Model has finished — build structured result ─────────────────
        elif finish_reason == "stop":
            final_text: str = choice.message.content or ""

            error_analysis = collected_tool_results.get("analyze_error_log", {})
            remediation = collected_tool_results.get("suggest_remediation", {})
            job_history = collected_tool_results.get("check_job_history", {})

            logger.info("[agent] Diagnosis complete for job=%s", job_id)

            return {
                "job_id": job_id,
                "error_type": error_analysis.get("error_type", "unknown"),
                "severity": error_analysis.get("severity", "unknown"),
                "likely_cause": error_analysis.get("likely_cause", ""),
                "remediation_steps": remediation.get("steps", []),
                "agent_summary": final_text,
                "job_history": job_history,
                "diagnosed_at": datetime.now(timezone.utc).isoformat(),
            }

        # ── Unexpected finish reason ──────────────────────────────────────
        else:
            logger.warning("[agent] Unexpected finish_reason=%s", finish_reason)
            break

    # Exceeded MAX_ITERATIONS — return a partial result
    logger.error("[agent] Max iterations (%d) reached for job=%s", MAX_ITERATIONS, job_id)
    return {
        "job_id": job_id,
        "error_type": "agent_loop_timeout",
        "severity": "unknown",
        "likely_cause": "Agent did not converge within iteration limit.",
        "remediation_steps": [],
        "agent_summary": "Diagnosis incomplete — agent loop exceeded iteration limit.",
        "job_history": {},
        "diagnosed_at": datetime.now(timezone.utc).isoformat(),
    }
