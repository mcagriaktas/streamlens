"""Integration tests for Kafka Connect connector discovery and details."""
import os

import pytest

from src.kafka import KafkaService
from src.topology import build_topology

KAFKA_CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")


@pytest.fixture()
def kafka_service():
    return KafkaService()


class TestFetchConnectors:
    def test_discovers_connector(self, kafka_service, test_connector, cluster_config):
        state = kafka_service.fetch_system_state(cluster_config)
        connector_ids = {c["id"] for c in state["connectors"]}
        assert f"connect:{test_connector}" in connector_ids

    def test_connector_type_is_sink(self, kafka_service, test_connector, cluster_config):
        state = kafka_service.fetch_system_state(cluster_config)
        match = [c for c in state["connectors"] if c["id"] == f"connect:{test_connector}"]
        assert len(match) >= 1
        assert match[0]["type"] == "sink"

    def test_connector_linked_to_topic(self, kafka_service, test_connector, cluster_config):
        state = kafka_service.fetch_system_state(cluster_config)
        match = [c for c in state["connectors"] if c["id"] == f"connect:{test_connector}"]
        assert len(match) >= 1
        assert match[0]["topic"] == "inttest-orders"


class TestConnectorDetails:
    def test_fetch_details(self, kafka_service, test_connector):
        details = kafka_service.fetch_connector_details(KAFKA_CONNECT_URL, test_connector)
        assert details["name"] == test_connector
        assert "FileStreamSinkConnector" in details["connectorClass"]
        assert details["config"]["topics"] == "inttest-orders"

    def test_details_contain_tasks(self, kafka_service, test_connector):
        details = kafka_service.fetch_connector_details(KAFKA_CONNECT_URL, test_connector)
        assert isinstance(details["tasks"], list)

    def test_nonexistent_connector_raises(self, kafka_service):
        with pytest.raises(RuntimeError):
            kafka_service.fetch_connector_details(KAFKA_CONNECT_URL, "no-such-connector")


class TestConnectorInTopology:
    def test_topology_contains_connector_node(self, test_connector, test_topics, cluster_config):
        result = build_topology(99, cluster_config)
        connector_nodes = [n for n in result["nodes"] if n["type"] == "connector"]
        connector_ids = {n["id"] for n in connector_nodes}
        assert f"connect:{test_connector}" in connector_ids

    def test_connector_edge_links_topic(self, test_connector, test_topics, cluster_config):
        result = build_topology(99, cluster_config)
        connector_edges = [
            e for e in result["edges"]
            if test_connector in (e.get("source", "") + e.get("target", ""))
        ]
        assert len(connector_edges) >= 1
        topics_in_edges = {e["source"] for e in connector_edges} | {e["target"] for e in connector_edges}
        assert "topic:inttest-orders" in topics_in_edges
