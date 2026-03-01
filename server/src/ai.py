import json
import logging
import os
import re
from typing import Any

from .kafka.metrics import (
    fetch_metrics_from_prometheus,
    format_metrics_for_prompt,
    get_metrics_catalog_summary,
)
from .storage import get_cluster

logger = logging.getLogger(__name__)

# Provider: "openai" | "gemini" | "anthropic" | "ollama" from env, or auto-detect from which API key is set
def _get_provider() -> str:
    explicit = (os.environ.get("AI_PROVIDER") or "").strip().lower()
    if explicit in ("openai", "gemini", "anthropic", "claude", "ollama"):
        return "anthropic" if explicit == "claude" else explicit
    openai_key = (os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY") or "").strip()
    gemini_key = (os.environ.get("AI_INTEGRATIONS_GEMINI_API_KEY") or "").strip()
    anthropic_key = (os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY") or "").strip()
    if openai_key and openai_key != "dummy":
        return "openai"
    if anthropic_key:
        return "anthropic"
    if gemini_key and gemini_key != "dummy":
        return "gemini"
    return "openai"  # default when no key set


def get_ai_status() -> dict:
    """Return the currently configured AI provider and whether it has an API key set."""
    provider = _get_provider()
    has_key = False
    model = None

    if provider == "openai":
        key = (os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY") or "").strip()
        has_key = bool(key) and key != "dummy"
        model = os.environ.get("AI_INTEGRATIONS_OPENAI_MODEL", "gpt-4o-mini")
    elif provider == "gemini":
        key = (os.environ.get("AI_INTEGRATIONS_GEMINI_API_KEY") or "").strip()
        has_key = bool(key) and key != "dummy"
        model = os.environ.get("AI_INTEGRATIONS_GEMINI_MODEL", "gemini-2.0-flash")
    elif provider == "anthropic":
        key = (os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY") or "").strip()
        has_key = bool(key)
        model = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_MODEL", "claude-3-5-haiku-20241022")
    elif provider == "ollama":
        has_key = True  # Ollama is local, no key needed
        model = os.environ.get("OLLAMA_MODEL", "llama3:latest")

    return {
        "provider": provider if has_key else None,
        "configured": has_key,
        "model": model if has_key else None,
    }

# Lazy OpenAI client (only when provider is openai)
_openai_client = None

def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        from openai import OpenAI
        _openai_client = OpenAI(
            api_key=os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY", "dummy"),
            base_url=os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL"),
        )
    return _openai_client

# Lazy Anthropic client (only when provider is anthropic)
_anthropic_client = None

def _get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        try:
            from anthropic import Anthropic
        except ImportError:
            raise RuntimeError(
                "Anthropic provider selected but anthropic is not installed. "
                "Install with: uv sync --extra anthropic  or  pip install anthropic"
            )
        api_key = (os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY") or "").strip()
        if not api_key:
            raise ValueError("AI_INTEGRATIONS_ANTHROPIC_API_KEY is not set")
        _anthropic_client = Anthropic(api_key=api_key)
    return _anthropic_client

# Lazy Ollama client (OpenAI-compatible API; only when provider is ollama)
_ollama_client = None

def _get_ollama_client():
    global _ollama_client
    if _ollama_client is None:
        from openai import OpenAI
        base = (os.environ.get("OLLAMA_BASE_URL") or "http://localhost:11434").strip().rstrip("/")
        _ollama_client = OpenAI(base_url=f"{base}/v1", api_key="ollama")
    return _ollama_client

# Lazy Gemini model (only when provider is gemini)
_gemini_model = None

def _get_gemini_model():
    global _gemini_model
    if _gemini_model is None:
        try:
            import google.generativeai as genai
        except ImportError:
            raise RuntimeError(
                "Gemini provider selected but google-generativeai is not installed. "
                "Install with: uv sync --extra gemini  or  pip install google-generativeai"
            )
        api_key = (os.environ.get("AI_INTEGRATIONS_GEMINI_API_KEY") or "").strip()
        if not api_key:
            raise ValueError("AI_INTEGRATIONS_GEMINI_API_KEY is not set")
        genai.configure(api_key=api_key)
        model_name = os.environ.get("AI_INTEGRATIONS_GEMINI_MODEL", "gemini-2.0-flash")
        _gemini_model = genai.GenerativeModel(model_name)
    return _gemini_model


SYSTEM_PROMPT = "You are a helpful Kafka expert. You are read-only: you must never create, update, or delete any Kafka resources (topics, consumers, producers, connectors, schemas, ACLs). If the user asks you to do any of these, decline politely and explain that StreamPilot is read-only."

# Phrases that indicate a mutating request (create/update/delete) — decline without calling the LLM
_MUTATING_PATTERNS = [
    r"\b(delete|remove|drop)\b.*\b(topic|consumer|producer|connector|schema|acl)\b",
    r"\b(topic|consumer|producer|connector|schema|acl)\b.*\b(delete|remove|drop)\b",
    r"\b(create|add|new)\b.*\b(topic|consumer|producer|connector|schema|acl)\b",
    r"\b(topic|consumer|producer|connector|schema|acl)\b.*\b(create|add)\b",
    r"\b(update|change|modify|alter)\b.*\b(topic|consumer|producer|connector|schema|acl)\b",
    r"\b(topic|consumer|producer|connector|schema|acl)\b.*\b(update|change|modify)\b",
    r"\b(produce|send|write)\s+(a\s+)?(message|event)\b",  # "produce a message" = write to topic
    r"\b(clear|reset|purge)\b.*\b(topic|offset|consumer)\b",
    r"\b(revoke|grant)\b.*\b(acl|permission)\b",
]
_MUTATING_RE = re.compile("|".join(f"({p})" for p in _MUTATING_PATTERNS), re.IGNORECASE)

READ_ONLY_DECLINE = {
    "answer": "StreamPilot is read-only. I can only answer questions about your current topology (e.g. which producers write to a topic, which consumers read from it, schemas, connectors). I cannot create, update, or delete topics, consumers, producers, connectors, schemas, or ACLs. To make changes, use your Kafka tools (kafka-topics.sh, kafka-consumer-groups.sh, Connect REST API, etc.) or your admin console.",
    "highlightNodes": [],
}


def _is_mutating_request(question: str) -> bool:
    """Return True if the question asks to create, update, or delete any Kafka resource."""
    if not (question or "").strip():
        return False
    q = question.strip()
    return bool(_MUTATING_RE.search(q))


PROMPT_TEMPLATE = """
You are StreamPilot, a Kafka topology reasoning assistant. You are READ-ONLY: you must not create, update, or delete any topics, consumers, producers, connectors, schemas, or ACLs. If the user asks you to do any of these, respond with a short polite decline and say you can only answer questions about the current topology.

You are given a graph with nodes and edges representing Kafka topics, producers, consumers, streams applications, connectors, and schemas.

IMPORTANT: The topology below may be PARTIAL — only a subset of topics are loaded at a time for performance.
If the user asks about a topic that is NOT in the data below, do NOT say it does not exist.
Instead, assume it may exist in the full cluster but is simply not loaded yet.
Still return the best-guess node ID in highlightNodes (e.g. "topic:treasury-hedging-topic") so the UI can search the full cluster and load it automatically.

Current Topology Graph JSON:
{topology_json}

{metrics_context}

User question:
{question}

Using the graph:
1. Analyze the nodes and edges to find relevant entities that answer the question.
2. Provide a clear, concise answer in plain English.
3. Return the exact node IDs (from the graph) of all relevant entities to highlight and zoom to in the UI.
4. If a topic/entity is NOT in the graph data above, still include its best-guess node ID in highlightNodes so the UI can search for it in the full cluster.

Important:
- For questions about "producers writing to X topic", include producer node IDs and the topic node ID
- For questions about "consumers of X topic", include consumer node IDs and the topic node ID
- For questions about "topics produced by X", include the producer/app node ID and all relevant topic node IDs
- For questions about schemas, include schema node IDs (format: "schema:subject-name") and their linked topic nodes
- For questions about connectors, include connector node IDs (format: "connect:connector-name") and their linked topic nodes
- For questions about ACLs on a topic, include ACL node IDs (format: "acl:topic:topicname") and the topic node
- Always include the full node ID as it appears in the graph (e.g., "topic:testtopic", "group:mygroup", "jmx:active-producer:testtopic", "schema:orders-value", "connect:file-source", "acl:topic:transactions-topic")
- When the user asks to "navigate to", "show me", "get me to", or "find" a specific topic, ALWAYS include "topic:<name>" in highlightNodes even if it is not in the graph data
- When the user asks about broker metrics, throughput, latency, replication, or cluster health, use the live metrics data provided above to answer with actual numbers.  If no metrics data is available, explain that Prometheus is not configured.

Respond with ONLY a single JSON object, no other text or markdown. Use this exact structure:
{{"answer": "Plain English explanation...", "highlightNodes": ["topic:orders", "group:checkout-consumer"]}}
"""


def _parse_json_from_text(text: str) -> dict:
    """Strip markdown code fences and parse JSON."""
    text = (text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```\s*$", "", text)
    return json.loads(text or "{}")


def _build_prompt(question: str, topology: dict, metrics_context: str = "") -> str:
    return PROMPT_TEMPLATE.format(
        topology_json=json.dumps(topology),
        question=question,
        metrics_context=metrics_context,
    )


def _query_openai(question: str, topology: dict, metrics_context: str = "") -> dict:
    client = _get_openai_client()
    prompt = _build_prompt(question, topology, metrics_context)
    response = client.chat.completions.create(
        model=os.environ.get("AI_INTEGRATIONS_OPENAI_MODEL", "gpt-4o-mini"),
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
    )
    content = response.choices[0].message.content or "{}"
    return json.loads(content)


def _query_gemini(question: str, topology: dict, metrics_context: str = "") -> dict:
    model = _get_gemini_model()
    prompt = _build_prompt(question, topology, metrics_context)
    response = model.generate_content(
        prompt,
        generation_config={"temperature": 0.2},
    )
    text = (response.text or "").strip()
    return _parse_json_from_text(text)


def _query_anthropic(question: str, topology: dict, metrics_context: str = "") -> dict:
    client = _get_anthropic_client()
    prompt = _build_prompt(question, topology, metrics_context)
    model = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_MODEL", "claude-3-5-haiku-20241022")
    message = client.messages.create(
        model=model,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    text = ""
    for block in message.content:
        if hasattr(block, "text") and block.text:
            text += block.text
    return _parse_json_from_text(text)


def _query_ollama(question: str, topology: dict, metrics_context: str = "") -> dict:
    client = _get_ollama_client()
    prompt = _build_prompt(question, topology, metrics_context)
    model = os.environ.get("OLLAMA_MODEL", "llama3.2")
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    )
    content = response.choices[0].message.content or "{}"
    return _parse_json_from_text(content)


_METRICS_KEYWORDS = re.compile(
    r"\b(metrics?|throughput|latency|bytes.?in|bytes.?out|messages?.?in|isr|"
    r"under.?replicated|offline.?partition|request.?rate|produce.?rate|"
    r"fetch.?rate|leader.?count|partition.?count|log.?size|replication|"
    r"broker.?health|cluster.?health|performance|load|traffic|"
    r"messages?.per.?sec|bytes?.per.?sec|"
    # request timing / rate patterns
    r"produce.?request|fetch.?request|request.?time|response.?time|"
    r"avg.?time|average.?time|total.?time|"
    # general broker / capacity
    r"how\s+many\s+partitions?|how\s+many\s+leaders?|how\s+many\s+brokers?|"
    r"controller|replica|broker.?state|"
    r"msg.?rate|message.?rate|"
    r"produce.?time|fetch.?time|consumer.?lag|"
    r"network.?idle|queue.?size)\b",
    re.IGNORECASE,
)


def _is_metrics_question(question: str) -> bool:
    """Return True if the question is about broker metrics / performance."""
    return bool(_METRICS_KEYWORDS.search(question or ""))


def _fetch_metrics_context(cluster_id: int | None) -> str:
    """Look up the cluster's prometheusUrl and fetch live metrics if available."""
    if not cluster_id:
        return ""
    try:
        cluster = get_cluster(cluster_id)
        if not cluster:
            return ""
        prometheus_url = (cluster.get("prometheusUrl") or "").strip().rstrip("/")
        if not prometheus_url:
            return get_metrics_catalog_summary() + "\n\nNote: Prometheus is not configured for this cluster. Configure prometheusUrl in the cluster settings to enable live metric queries."

        metrics = fetch_metrics_from_prometheus(prometheus_url)
        if metrics:
            return format_metrics_for_prompt(metrics)
        return get_metrics_catalog_summary() + "\n\nNote: Prometheus returned no metric data. The exporter may not be running."
    except Exception as e:
        logger.warning("Failed to fetch metrics context: %s", e)
        return ""


def query_topology(question: str, topology: dict, cluster_id: int | None = None) -> dict:
    if _is_mutating_request(question):
        return dict(READ_ONLY_DECLINE)

    metrics_context = ""
    if _is_metrics_question(question):
        metrics_context = _fetch_metrics_context(cluster_id)

    provider = _get_provider()
    try:
        if provider == "gemini":
            result = _query_gemini(question, topology, metrics_context)
        elif provider == "anthropic":
            result = _query_anthropic(question, topology, metrics_context)
        elif provider == "ollama":
            result = _query_ollama(question, topology, metrics_context)
        else:
            result = _query_openai(question, topology, metrics_context)
        if "highlightNodes" not in result:
            result["highlightNodes"] = []
        if "answer" not in result:
            result["answer"] = "No answer generated."
        return result
    except Exception as e:
        print(f"AI Query failed ({provider}): {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

        if provider == "openai":
            api_key = (os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY") or "").strip()
            if not api_key or api_key == "dummy":
                error_msg = (
                    "AI assistant not configured. Set AI_INTEGRATIONS_OPENAI_API_KEY, or use Gemini (AI_PROVIDER=gemini), "
                    "Anthropic (AI_PROVIDER=anthropic), or Ollama (AI_PROVIDER=ollama). See docs/AI_SETUP.md."
                )
            else:
                error_msg = f"I couldn't process your request: {str(e)}"
        elif provider == "ollama":
            error_msg = (
                "Ollama not reachable. Set AI_PROVIDER=ollama, ensure Ollama is running (e.g. ollama serve), "
                "and optionally OLLAMA_BASE_URL (default http://localhost:11434), OLLAMA_MODEL (default llama3.2). See docs/AI_SETUP.md."
            )
            if "connection" not in str(e).lower() and "refused" not in str(e).lower():
                error_msg = f"I couldn't process your request: {str(e)}"
        elif provider == "anthropic":
            api_key = (os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY") or "").strip()
            if not api_key:
                error_msg = (
                    "Anthropic not configured. Set AI_INTEGRATIONS_ANTHROPIC_API_KEY (and optionally AI_PROVIDER=anthropic or claude). "
                    "Install with: uv sync --extra anthropic. See docs/AI_SETUP.md."
                )
            else:
                error_msg = f"I couldn't process your request: {str(e)}"
        else:
            api_key = (os.environ.get("AI_INTEGRATIONS_GEMINI_API_KEY") or "").strip()
            if not api_key:
                error_msg = (
                    "Gemini not configured. Set AI_INTEGRATIONS_GEMINI_API_KEY (and optionally AI_PROVIDER=gemini). "
                    "See docs/AI_SETUP.md."
                )
            else:
                error_msg = f"I couldn't process your request: {str(e)}"

        return {
            "answer": error_msg,
            "highlightNodes": [],
        }
