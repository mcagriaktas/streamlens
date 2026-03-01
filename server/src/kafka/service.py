"""KafkaService — orchestrates fetching cluster state from multiple sources."""
import logging
from typing import Any

from confluent_kafka.admin import AdminClient

from .acls import fetch_topic_acls
from .config import client_config
from .connectors import fetch_connector_details, fetch_connectors
from .consumers import fetch_consumer_groups, fetch_consumer_lag
from .producers import (
    detect_producers_by_offset_change,
    fetch_jmx_producers,
    fetch_prometheus_broker_producers,
    fetch_prometheus_producers,
)
from .schemas import fetch_schema_details, fetch_schemas
from .topics import fetch_topic_details, fetch_topics, produce_message

logger = logging.getLogger(__name__)

_EMPTY_STATE_KEYS = ("topics", "producers", "consumers", "streams", "connectors", "schemas", "acls")


class KafkaService:
    """Fetches real cluster state from Kafka broker, Connect, and Schema Registry."""

    @staticmethod
    def _empty_state() -> dict[str, Any]:
        return {k: [] for k in _EMPTY_STATE_KEYS}

    def check_cluster_health(self, cluster: dict[str, Any]) -> dict[str, Any]:
        bootstrap = cluster.get("bootstrapServers") or ""
        if not bootstrap:
            return {"online": False, "error": "No bootstrap servers configured", "clusterMode": None}

        bootstrap_list = [s.strip() for s in bootstrap.split(",") if s.strip()]
        if not bootstrap_list:
            return {"online": False, "error": "Invalid bootstrap servers", "clusterMode": None}

        try:
            admin = AdminClient(client_config(cluster))
            metadata = admin.list_topics(timeout=5)
            if metadata is not None:
                mode: str | None = None
                if getattr(metadata, "topics", None):
                    mode = "kraft" if "__cluster_metadata" in metadata.topics else "zookeeper"
                return {"online": True, "error": None, "clusterMode": mode}
            return {"online": False, "error": "Failed to retrieve cluster metadata", "clusterMode": None}
        except Exception as e:
            error_msg = str(e)
            if "timed out" in error_msg.lower():
                error_msg = "Connection timeout - cluster unreachable"
            elif "failed to resolve" in error_msg.lower():
                error_msg = "Cannot resolve bootstrap servers"
            return {"online": False, "error": error_msg, "clusterMode": None}

    def fetch_system_state(self, cluster: dict[str, Any]) -> dict[str, Any]:
        bootstrap = cluster.get("bootstrapServers") or ""
        if not bootstrap:
            return self._empty_state()
        bootstrap_list = [s.strip() for s in bootstrap.split(",") if s.strip()]
        if not bootstrap_list:
            return self._empty_state()

        cfg = client_config(cluster)
        state: dict[str, Any] = self._empty_state()

        try:
            admin = AdminClient(cfg)
            state["topics"] = fetch_topics(admin)
            state["consumers"] = fetch_consumer_groups(admin, cfg)
            state["producers"] = self._collect_producers(cluster, admin, cfg, state["topics"])

            topic_names = [t["name"] for t in state["topics"] if not (t.get("name") or "").startswith("__")]
            state["acls"] = fetch_topic_acls(admin, topic_names)
            state["streams"] = []
        except Exception as e:
            logger.exception("Kafka broker error: %s", e)
            raise RuntimeError(f"Cannot connect to Kafka at {bootstrap}: {e}") from e

        connect_url = (cluster.get("connectUrl") or "").strip().rstrip("/")
        if connect_url:
            try:
                state["connectors"] = fetch_connectors(connect_url)
            except Exception as e:
                logger.warning("Kafka Connect unreachable at %s: %s", connect_url, e)

        schema_url = (cluster.get("schemaRegistryUrl") or "").strip().rstrip("/")
        if schema_url:
            try:
                state["schemas"] = fetch_schemas(schema_url)
            except Exception as e:
                logger.warning("Schema Registry unreachable at %s: %s", schema_url, e)

        return state

    # --- Delegating convenience methods (public API kept on KafkaService) ---

    def fetch_topic_details(self, cluster: dict[str, Any], topic_name: str, include_messages: bool = False) -> dict[str, Any]:
        return fetch_topic_details(cluster, topic_name, include_messages)

    def produce_message(self, cluster: dict[str, Any], topic_name: str, value: str, key: str | None = None) -> dict[str, Any]:
        return produce_message(cluster, topic_name, value, key)

    def fetch_consumer_lag(self, cluster: dict[str, Any], group_id: str) -> dict[str, Any]:
        return fetch_consumer_lag(cluster, group_id)

    def fetch_connector_details(self, connect_url: str, connector_name: str) -> dict[str, Any]:
        return fetch_connector_details(connect_url, connector_name)

    def fetch_schema_details(self, schema_url: str, subject: str, version: str | None = None) -> dict[str, Any]:
        return fetch_schema_details(schema_url, subject, version)

    # --- Private helpers ---

    @staticmethod
    def _collect_producers(
        cluster: dict[str, Any], admin: AdminClient, cfg: dict, topics: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        producers: list[dict[str, Any]] = []
        detected = False

        prometheus_url = (cluster.get("prometheusUrl") or "").strip().rstrip("/")

        # Option 1: Prometheus client-side (per-client-id + topic, best granularity)
        if prometheus_url:
            prom_producers = fetch_prometheus_producers(prometheus_url)
            if prom_producers:
                producers.extend(prom_producers)
                detected = True

        # Option 2: Prometheus broker-side (per-topic, same as JMX but via Prometheus)
        if not detected and prometheus_url:
            topic_names = [t["name"] for t in topics if not (t.get("name") or "").startswith("__")]
            broker_producers = fetch_prometheus_broker_producers(prometheus_url, topic_names)
            if broker_producers:
                producers.extend(broker_producers)
                detected = True

        # Option 3: Broker JMX (per-topic, only if Prometheus is not configured)
        if not detected:
            jmx_host = cluster.get("jmxHost")
            jmx_port = cluster.get("jmxPort")
            if jmx_host and jmx_port:
                jmx_producers = fetch_jmx_producers(jmx_host, jmx_port)
                if jmx_producers:
                    producers.extend(jmx_producers)
                    detected = True

        # Option 4: Offset-change heuristic (per-topic, needs two syncs)
        if not detected:
            producers.extend(detect_producers_by_offset_change(cluster.get("id", 0), cfg, topics))

        logger.info("Topology state: %d producers (will appear in UI after Sync)", len(producers))
        return producers
