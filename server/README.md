# StreamLens Server

FastAPI backend for StreamLens.

## Setup

```bash
cd server
uv sync
```

## Run

```bash
uv run uvicorn main:app --reload --port 5000
```

Or: `uv run python main.py`

## Configuration

**Clusters** are stored in `data/clusters.json` (no database). Override the path with `CLUSTERS_JSON`. See the [project README](../README.md) for cluster config and SSL options.

**AI Assistant** (optional) — supports OpenAI, Gemini, Anthropic, and Ollama. See [docs/AI_SETUP.md](../docs/AI_SETUP.md).

**Topology** is built from live cluster data: topics and consumer groups from the broker, plus connectors (Kafka Connect) and schemas (Schema Registry) when configured. Snapshots refresh automatically every minute or on manual Sync.
