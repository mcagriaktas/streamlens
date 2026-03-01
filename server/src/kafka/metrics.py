"""Curated Kafka broker metrics catalog for AI-assisted querying via Prometheus."""
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

METRICS_CATALOG: list[dict[str, Any]] = [
    # --- Cluster Health ---
    {
        "name": "under_replicated_partitions",
        "category": "Cluster Health",
        "description": "Number of partitions where replicas are not fully in sync. Non-zero means potential data loss risk.",
        "queries": [
            "sum(kafka_server_replicamanager_underreplicatedpartitions)",
        ],
    },
    {
        "name": "active_controller_count",
        "category": "Cluster Health",
        "description": "Number of active controllers in the cluster. Should always be exactly 1.",
        "queries": [
            "sum(kafka_controller_kafkacontroller_activecontrollercount)",
        ],
    },
    {
        "name": "offline_partitions_count",
        "category": "Cluster Health",
        "description": "Number of partitions without an active leader. Non-zero means those partitions are unavailable.",
        "queries": [
            "sum(kafka_controller_kafkacontroller_offlinepartitionscount)",
        ],
    },
    # --- Throughput ---
    {
        "name": "messages_in_per_sec",
        "category": "Throughput",
        "description": "Total messages produced per second across all topics (1-minute rate).",
        "queries": [
            "kafka_server_brokertopicmetrics_messagesinpersec_rate1m",
            'sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_count[5m]))',
        ],
    },
    {
        "name": "messages_in_total",
        "category": "Throughput",
        "description": "Total cumulative messages produced across all topics.",
        "queries": [
            "kafka_server_brokertopicmetrics_messagesinpersec_total",
            "sum(kafka_server_brokertopicmetrics_messagesinpersec_count)",
        ],
    },
    {
        "name": "messages_in_per_sec_by_topic",
        "category": "Throughput",
        "description": "Messages produced per second broken down by topic.",
        "queries": [
            '{__name__=~"kafka_server_brokertopicmetrics_messagesinpersec_topic_.+_rate1m"}',
            'sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesinpersec_count{topic!=""}[5m]))',
        ],
    },
    {
        "name": "bytes_in_per_sec",
        "category": "Throughput",
        "description": "Total bytes produced per second across all brokers (1-minute rate).",
        "queries": [
            "kafka_server_brokertopicmetrics_bytesinpersec_rate1m",
            "sum(rate(kafka_server_brokertopicmetrics_bytesinpersec_count[5m]))",
        ],
    },
    {
        "name": "bytes_out_per_sec",
        "category": "Throughput",
        "description": "Total bytes consumed per second across all brokers (1-minute rate).",
        "queries": [
            "kafka_server_brokertopicmetrics_bytesoutpersec_rate1m",
            "sum(rate(kafka_server_brokertopicmetrics_bytesoutpersec_count[5m]))",
        ],
    },
    # --- Request Performance ---
    {
        "name": "produce_request_rate",
        "category": "Request Performance",
        "description": "Produce requests per second across all brokers.",
        "queries": [
            'sum(kafka_network_requestmetrics_requestspersec_rate1m{request=~"Produce.*"})',
            'sum(rate(kafka_network_requestmetrics_requestspersec_count{request="Produce"}[5m]))',
        ],
    },
    {
        "name": "fetch_consumer_request_rate",
        "category": "Request Performance",
        "description": "Fetch (consumer) requests per second across all brokers.",
        "queries": [
            'sum(kafka_network_requestmetrics_requestspersec_rate1m{request=~"FetchConsumer.*|Fetch.*"})',
            'sum(rate(kafka_network_requestmetrics_requestspersec_count{request="FetchConsumer"}[5m]))',
        ],
    },
    {
        "name": "produce_total_time_ms_avg",
        "category": "Request Performance",
        "description": "Average time (ms) to handle a produce request end-to-end (queue + local + remote + response).",
        "queries": [
            'avg(kafka_network_requestmetrics_totaltimems_mean{request=~"Produce.*"})',
            'avg(kafka_network_requestmetrics_totaltimems{request="Produce",attribute="Mean"})',
        ],
    },
    {
        "name": "fetch_total_time_ms_avg",
        "category": "Request Performance",
        "description": "Average time (ms) to handle a fetch request end-to-end.",
        "queries": [
            'avg(kafka_network_requestmetrics_totaltimems_mean{request=~"FetchConsumer.*|Fetch.*"})',
            'avg(kafka_network_requestmetrics_totaltimems{request="FetchConsumer",attribute="Mean"})',
        ],
    },
    # --- Broker Resources ---
    {
        "name": "partition_count",
        "category": "Broker Resources",
        "description": "Total number of partitions across all brokers (including replicas).",
        "queries": [
            "sum(kafka_server_replicamanager_partitioncount)",
        ],
    },
    {
        "name": "leader_count",
        "category": "Broker Resources",
        "description": "Number of partition leaders per broker. Imbalanced leader counts indicate uneven load.",
        "queries": [
            "kafka_server_replicamanager_leadercount",
        ],
    },
    {
        "name": "log_size_bytes",
        "category": "Broker Resources",
        "description": "Total log size in bytes across all brokers and topics.",
        "queries": [
            'sum({__name__=~"kafka_log_log_size.*"})',
            "sum(kafka_log_log_size)",
        ],
    },
    # --- Replication ---
    {
        "name": "isr_shrinks_per_sec",
        "category": "Replication",
        "description": "Rate of in-sync replica shrinks per second. Frequent shrinks indicate broker or network issues.",
        "queries": [
            "sum(kafka_server_replicamanager_isrshrinkspersec_rate1m)",
            "sum(rate(kafka_server_replicamanager_isrshrinkspersec_count[5m]))",
        ],
    },
    {
        "name": "isr_expands_per_sec",
        "category": "Replication",
        "description": "Rate of in-sync replica expansions per second. Expands follow shrinks as replicas catch up.",
        "queries": [
            "sum(kafka_server_replicamanager_isrexpandspersec_rate1m)",
            "sum(rate(kafka_server_replicamanager_isrexpandspersec_count[5m]))",
        ],
    },
]


def get_metrics_catalog_summary() -> str:
    """Return a text summary of available metrics for inclusion in AI prompts."""
    lines = ["Available Kafka broker metrics (query by name):"]
    for m in METRICS_CATALOG:
        lines.append(f"  - {m['name']}: {m['description']} [{m['category']}]")
    return "\n".join(lines)


def _extract_topic_from_metric_name(name: str) -> str | None:
    """Extract topic name from JMX-style metric names where the topic is embedded.

    e.g. kafka_server_brokertopicmetrics_messagesinpersec_topic_audit_log_topic_rate1m
    -> audit_log_topic
    """
    import re
    match = re.search(r"_topic_(.+?)_(rate1m|total)$", name)
    return match.group(1) if match else None


def fetch_metrics_from_prometheus(
    prometheus_url: str, metric_names: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Fetch current values for the requested metrics (or all if none specified).

    Each metric definition has multiple query variants (for different JMX Exporter
    configurations). The first query that returns data wins.
    """
    targets = METRICS_CATALOG
    if metric_names:
        name_set = set(metric_names)
        targets = [m for m in METRICS_CATALOG if m["name"] in name_set]

    results: list[dict[str, Any]] = []
    url = f"{prometheus_url}/api/v1/query"

    try:
        with httpx.Client(timeout=10.0) as client:
            for m in targets:
                try:
                    data: list[dict] = []
                    for query in m["queries"]:
                        resp = client.get(url, params={"query": query})
                        resp.raise_for_status()
                        data = resp.json().get("data", {}).get("result", [])
                        if data:
                            break

                    if not data:
                        results.append({
                            "name": m["name"],
                            "category": m["category"],
                            "description": m["description"],
                            "value": "no data",
                        })
                        continue

                    if len(data) == 1:
                        val = data[0].get("value", [0, "0"])[1]
                        results.append({
                            "name": m["name"],
                            "category": m["category"],
                            "description": m["description"],
                            "value": val,
                        })
                    else:
                        noise_keys = {"__name__", "instance", "job"}
                        breakdown = []
                        for series in data:
                            metric_labels = series.get("metric", {})
                            labels = {k: v for k, v in metric_labels.items() if k not in noise_keys}
                            metric_name = metric_labels.get("__name__", "")
                            topic = _extract_topic_from_metric_name(metric_name)
                            if topic and "topic" not in labels:
                                labels["topic"] = topic
                            val = series.get("value", [0, "0"])[1]
                            if labels:
                                breakdown.append({"labels": labels, "value": val})
                        results.append({
                            "name": m["name"],
                            "category": m["category"],
                            "description": m["description"],
                            "breakdown": breakdown if breakdown else None,
                            **({"value": data[0].get("value", [0, "0"])[1]} if not breakdown else {}),
                        })
                except Exception as e:
                    logger.debug("Failed to fetch metric %s: %s", m["name"], e)
                    results.append({
                        "name": m["name"],
                        "category": m["category"],
                        "description": m["description"],
                        "value": f"error: {e}",
                    })
    except httpx.ConnectError:
        logger.info("Prometheus not reachable at %s", prometheus_url)
    except Exception as e:
        logger.warning("Prometheus metrics fetch failed: %s", e)

    return results


def format_metrics_for_prompt(metrics: list[dict[str, Any]]) -> str:
    """Format fetched metric results into text for the AI prompt."""
    if not metrics:
        return ""

    lines = ["Live Kafka Broker Metrics (from Prometheus):"]
    current_cat = ""
    for m in metrics:
        if m["category"] != current_cat:
            current_cat = m["category"]
            lines.append(f"\n  [{current_cat}]")

        if m.get("breakdown"):
            lines.append(f"  {m['name']}:")
            for entry in m["breakdown"][:20]:
                label_str = ", ".join(f"{k}={v}" for k, v in entry["labels"].items())
                lines.append(f"    {label_str}: {entry['value']}")
            if len(m["breakdown"]) > 20:
                lines.append(f"    ... and {len(m['breakdown']) - 20} more")
        else:
            lines.append(f"  {m['name']}: {m.get('value', 'N/A')}")

    return "\n".join(lines)
