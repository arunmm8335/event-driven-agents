# Local Mini-Kafka + AI Diagnostics Simulator

A fully local, production-style event-driven AI diagnostics system.  
No cloud services. No company infrastructure. Just your machine.

```
Docker Kafka
     ↓
Topic: pipeline_failures
     ↓
Python Consumer (consumer_agent.py)
     ↓
Ollama LLM Agent — tool-calling loop (agent.py)
     ├─ analyze_error_log()
     ├─ check_job_history()
     └─ suggest_remediation()
     ↓
Topic: pipeline_diagnostics
     ↓
Python Consumer (consumer_output.py) — pretty-prints results
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Docker Desktop | Running with Compose v2 |
| Ollama | https://ollama.com — install and run `ollama serve` |
| Python 3.10+ | With a virtualenv activated |

---

## Quick Start

### 1 — Pull the Ollama model

```bash
ollama pull llama3.2
```

> Alternatives: `llama3.1` (better reasoning), `qwen2.5:7b` (excellent tool use).  
> Update `MODEL` in `agent.py` if you switch.

### 2 — Start Kafka

```bash
docker compose up -d
```

Wait ~15 s for Kafka to become healthy:

```bash
docker compose ps       # both services should show "healthy"
```

### 3 — Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4 — Open three terminals

**Terminal A — diagnostic output consumer** (start this first so you see results immediately):
```bash
python consumer_output.py
```

**Terminal B — agent consumer** (reads failures, runs LLM, publishes diagnoses):
```bash
python consumer_agent.py
```

**Terminal C — producer** (sends fake failure events):
```bash
python producer.py
```

Add `--loop` to keep sending events continuously:
```bash
python producer.py --loop
```

---

## Project Structure

```
project/
├── docker-compose.yml     Kafka + Zookeeper (Confluent images)
├── requirements.txt
│
├── schemas.py             Dataclass definitions for both Kafka topics
├── tools.py               Mock tool functions + OpenAI tool definitions
├── agent.py               LLM tool-calling loop (Ollama via openai SDK)
│
├── producer.py            Sends fake pipeline_failures events
├── consumer_agent.py      Consumes failures → runs agent → publishes diagnoses
└── consumer_output.py     Consumes pipeline_diagnostics → pretty-prints results
```

---

## Topics

| Topic | Direction | Schema |
|---|---|---|
| `pipeline_failures` | Producer → Agent Consumer | `{job_id, error_log, source, timestamp}` |
| `pipeline_diagnostics` | Agent Consumer → Output Consumer | `{job_id, error_type, severity, likely_cause, remediation_steps, agent_summary, job_history, diagnosed_at}` |

---

## Mock Tools

| Tool | What it simulates |
|---|---|
| `analyze_error_log(log_text)` | Classifies error type + severity |
| `check_job_history(job_id)` | Returns historical run stats |
| `suggest_remediation(error_type)` | Returns actionable fix steps |

All mock data lives in `tools.py` — extend it freely.

---

## Changing the Model

Edit `agent.py`:
```python
MODEL = "llama3.2"   # ← change this
```

Then pull the model:
```bash
ollama pull qwen2.5:7b
```

---

## Stopping Everything

```bash
docker compose down        # stop Kafka
# Ctrl-C in each Python terminal
```

---

## Optional Next Steps

- Add **Pydantic validation** on incoming events (replace bare dicts)
- Add **streaming** via `stream=True` in the Ollama call
- Add **token usage tracking** per diagnosis
- Add **retry / dead-letter queue** for failed agent calls
- Add **Kafka Schema Registry** (also available via Confluent Docker image)
- Replace mock tools with real DB / API calls
