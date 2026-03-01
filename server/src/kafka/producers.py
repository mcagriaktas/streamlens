"""Producer detection via Prometheus, JMX, ACLs, and offset-change heuristics."""
import logging
import re
import time
from typing import Any

import httpx
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

logger = logging.getLogger(__name__)

_offset_baseline: dict[int, dict[str, Any]] = {}

_PROMETHEUS_CLIENT_QUERY = (
    "sum by (client_id, topic) "
    "(kafka_producer_topic_metrics_record_send_total)"
)

_PROMETHEUS_BROKER_QUERIES = [
    '{__name__=~"kafka_server_brokertopicmetrics_messagesinpersec_topic_.+_rate1m"}',
    'sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesinpersec_count{topic!=""}[5m]))',
]


def fetch_prometheus_producers(prometheus_url: str) -> list[dict[str, Any]]:
    """Discover individual producer client IDs via a Prometheus instant query.

    Queries kafka_producer_topic_metrics_record_send_total (exposed by
    the JMX Exporter running on each producer) and groups results by
    client_id and topic.
    """
    producers: list[dict[str, Any]] = []
    url = f"{prometheus_url}/api/v1/query"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url, params={"query": _PROMETHEUS_CLIENT_QUERY})
            resp.raise_for_status()
            data = resp.json()

        results = data.get("data", {}).get("result", [])
        if not results:
            logger.info("Prometheus: no producer-topic-metrics found")
            return producers

        client_topic_map: dict[str, set[str]] = {}
        for series in results:
            metric = series.get("metric", {})
            client_id = metric.get("client_id", "")
            topic = metric.get("topic", "")
            if not client_id or not topic or topic.startswith("__"):
                continue
            value = float(series.get("value", [0, "0"])[1])
            if value > 0:
                client_topic_map.setdefault(client_id, set()).add(topic)

        for client_id, topics in sorted(client_topic_map.items()):
            producers.append({
                "id": f"prometheus:{client_id}",
                "producesTo": sorted(topics),
                "source": "prometheus",
                "label": client_id,
                "clientId": client_id,
            })

        logger.info("Prometheus: detected %d producer(s) with client IDs", len(producers))

    except httpx.HTTPStatusError as e:
        logger.warning("Prometheus query failed (HTTP %s): %s", e.response.status_code, e)
    except httpx.ConnectError:
        logger.info("Prometheus not reachable at %s", prometheus_url)
    except Exception as e:
        logger.warning("Prometheus producer fetch failed: %s", e)

    return producers


def _extract_topic_from_metric_name(name: str) -> str | None:
    """Extract topic name from broker metric names with embedded topic.

    e.g. kafka_server_brokertopicmetrics_messagesinpersec_topic_audit_log_topic_rate1m
    -> audit_log_topic
    """
    match = re.search(r"_topic_(.+?)_(rate1m|total)$", name)
    return match.group(1) if match else None


def fetch_prometheus_broker_producers(
    prometheus_url: str, known_topics: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Detect active producers from broker-side Prometheus metrics (per topic).

    Queries kafka_server_brokertopicmetrics_messagesinpersec to find topics
    with non-zero message rates. Same granularity as JMX but via Prometheus.

    The JMX Exporter replaces hyphens with underscores in metric names, so
    known_topics is used to map back to real Kafka topic names.
    """
    producers: list[dict[str, Any]] = []
    url = f"{prometheus_url}/api/v1/query"

    normalized_to_real: dict[str, str] = {}
    if known_topics:
        for t in known_topics:
            normalized_to_real[t.replace("-", "_")] = t

    try:
        with httpx.Client(timeout=10.0) as client:
            results: list[dict] = []
            for query in _PROMETHEUS_BROKER_QUERIES:
                resp = client.get(url, params={"query": query})
                resp.raise_for_status()
                results = resp.json().get("data", {}).get("result", [])
                if results:
                    break

        if not results:
            logger.info("Prometheus broker metrics: no per-topic messagesinpersec found")
            return producers

        active_topics: set[str] = set()
        for series in results:
            metric = series.get("metric", {})
            topic = metric.get("topic", "")
            if not topic:
                topic = _extract_topic_from_metric_name(metric.get("__name__", "")) or ""
            if not topic or topic.startswith("__") or topic.startswith("connect_"):
                continue
            value = float(series.get("value", [0, "0"])[1])
            if value > 0:
                real_name = normalized_to_real.get(topic, topic)
                active_topics.add(real_name)

        for topic in sorted(active_topics):
            producers.append({
                "id": f"prometheus-broker:active-producer:{topic}",
                "producesTo": [topic],
                "source": "prometheus-broker",
                "label": f"Active Producer → {topic}",
            })

        logger.info("Prometheus broker metrics: detected producers for %d topic(s)", len(active_topics))

    except httpx.HTTPStatusError as e:
        logger.warning("Prometheus broker query failed (HTTP %s): %s", e.response.status_code, e)
    except httpx.ConnectError:
        logger.info("Prometheus not reachable at %s", prometheus_url)
    except Exception as e:
        logger.warning("Prometheus broker producer fetch failed: %s", e)

    return producers


def fetch_jmx_producers(jmx_host: str, jmx_port: int) -> list[dict[str, Any]]:
    """Detect active producers from Kafka broker JMX metrics (MessagesInPerSec)."""
    producers: list[dict[str, Any]] = []
    try:
        from jmxquery import JMXConnection, JMXQuery

        logger.info("Connecting to JMX at %s:%s", jmx_host, jmx_port)
        jmx_connection = JMXConnection(f"service:jmx:rmi:///jndi/rmi://{jmx_host}:{jmx_port}/jmxrmi")

        topic_metrics_query = JMXQuery(
            "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*",
            metric_name="Count",
        )
        metrics = jmx_connection.query([topic_metrics_query])
        logger.debug("JMX: Retrieved %d metric(s)", len(metrics))

        active_topics = _parse_active_topics(metrics)

        try:
            client_query = JMXQuery(
                "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce",
                metric_name="Count",
            )
            client_metrics = jmx_connection.query([client_query])
            if client_metrics and client_metrics[0].value and float(client_metrics[0].value) > 0:
                logger.info("JMX: Active produce requests detected (rate: %s)", client_metrics[0].value)
        except Exception as e:
            logger.debug("Could not query client metrics: %s", e)

        if active_topics:
            logger.info("JMX detected producers for %d topic(s)", len(active_topics))
            for topic in sorted(active_topics):
                producers.append({
                    "id": f"jmx:active-producer:{topic}",
                    "producesTo": [topic],
                    "source": "jmx",
                    "label": f"Active Producer → {topic}",
                })
        else:
            logger.info("JMX: No active producers detected")

    except ImportError:
        logger.warning("jmxquery library not installed. Install with: pip install jmxquery")
    except Exception as e:
        logger.warning("JMX producer fetch failed (JMX might not be enabled): %s", e)
    return producers


def fetch_acl_producers(admin: AdminClient) -> list[dict[str, Any]]:
    """Detect potential producers from ACL WRITE permissions."""
    producers: list[dict[str, Any]] = []
    try:
        from confluent_kafka.admin import (
            AclBindingFilter, ResourceType, ResourcePatternType, AclOperation, AclPermissionType,
        )

        acl_filter = AclBindingFilter(
            restype=ResourceType.TOPIC, name=None,
            resource_pattern_type=ResourcePatternType.ANY,
            principal=None, host=None,
            operation=AclOperation.WRITE,
            permission_type=AclPermissionType.ALLOW,
        )
        result = admin.describe_acls(acl_filter, request_timeout=10)
        acls = result.result()

        principal_topics: dict[str, set[str]] = {}
        for acl in acls:
            topic = acl.resource_name
            if topic.startswith("__"):
                continue
            clean = acl.principal.replace("User:", "").replace("ServiceAccount:", "")
            principal_topics.setdefault(clean, set()).add(topic)

        for principal, topics in principal_topics.items():
            if topics:
                producers.append({
                    "id": f"acl:{principal}",
                    "producesTo": sorted(topics),
                    "source": "acl",
                    "principal": principal,
                })
        logger.info("Found %d potential producers from ACLs", len(producers))
    except ImportError:
        logger.debug("ACL classes not available in confluent-kafka version")
    except Exception as e:
        logger.debug("ACL fetch failed (ACLs might not be enabled): %s", e)
    return producers


def detect_producers_by_offset_change(
    cluster_id: int, client_cfg: dict, topics: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Detect active producers by comparing high-watermarks across refreshes.

    First call records a baseline; subsequent calls detect increases.
    """
    producers: list[dict[str, Any]] = []
    if not topics:
        return producers

    try:
        consumer = Consumer({**client_cfg, "group.id": f"__streamlens-offset-probe-{cluster_id}"})
        current_offsets: dict[str, int] = {}

        for t in topics:
            topic_name = t.get("name") or ""
            if not topic_name or topic_name.startswith("__"):
                continue
            for p in range(t.get("partitions") or 0):
                try:
                    tp = TopicPartition(topic_name, p)
                    _low, high = consumer.get_watermark_offsets(tp, cached=False, timeout=2.0)
                    current_offsets[f"{topic_name}:{p}"] = high
                except Exception:
                    pass

        consumer.close()
        now = time.time()
        prev = _offset_baseline.get(cluster_id)

        if prev is None:
            _offset_baseline[cluster_id] = {**current_offsets, "_ts": now}
            logger.info(
                "Offset detection [cluster %d]: baseline recorded (%d partitions).",
                cluster_id, len(current_offsets),
            )
            return producers

        elapsed = now - prev.get("_ts", 0)
        active_topics: set[str] = set()
        for key, cur_high in current_offsets.items():
            prev_high = prev.get(key)
            if prev_high is not None and cur_high > prev_high:
                active_topics.add(key.rsplit(":", 1)[0])

        _offset_baseline[cluster_id] = {**current_offsets, "_ts": now}

        if active_topics:
            logger.info(
                "Offset detection [cluster %d]: %d topic(s) with active producers (%.0fs): %s",
                cluster_id, len(active_topics), elapsed, sorted(active_topics),
            )
            for topic in sorted(active_topics):
                producers.append({
                    "id": f"offset:active-producer:{topic}",
                    "producesTo": [topic],
                    "source": "offset",
                    "label": f"Active Producer → {topic}",
                })
        else:
            logger.info("Offset detection [cluster %d]: no offset changes (%.0fs window).", cluster_id, elapsed)

    except Exception as e:
        logger.warning("Offset-based producer detection failed for cluster %d: %s", cluster_id, e)

    return producers


def _parse_active_topics(metrics) -> set[str]:
    """Extract topic names with active message flow from JMX metrics."""
    active: set[str] = set()
    for metric in metrics:
        try:
            if getattr(metric, "attribute", "") != "Count":
                continue
            mbean_name = getattr(metric, "mBeanName", "")
            if not mbean_name and hasattr(metric, "to_query_string"):
                mbean_name = metric.to_query_string()
            match = re.search(r"topic=([^,/\]]+)", mbean_name)
            if not match:
                continue
            topic = match.group(1)
            if topic.startswith("__") or topic == "*":
                continue
            value = 0.0
            if metric.value is None:
                value = 0
            elif isinstance(metric.value, (int, float)):
                value = float(metric.value)
            else:
                value = float(str(metric.value).split()[0])
            if value > 0:
                active.add(topic)
                logger.info("JMX: Topic '%s' has active producers (count: %s)", topic, value)
        except (ValueError, IndexError, AttributeError) as e:
            logger.debug("Could not parse metric: %s", e)
    return active
