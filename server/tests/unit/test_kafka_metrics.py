"""Unit tests for server/src/kafka/metrics.py."""
from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.kafka.metrics import (
    METRICS_CATALOG,
    _extract_topic_from_metric_name,
    fetch_metrics_from_prometheus,
    format_metrics_for_prompt,
    get_metrics_catalog_summary,
)


class TestMetricsCatalog:
    """Tests for the curated metrics catalog."""

    def test_catalog_not_empty(self):
        assert len(METRICS_CATALOG) > 0

    def test_each_metric_has_required_fields(self):
        for m in METRICS_CATALOG:
            assert "name" in m, f"Metric missing 'name': {m}"
            assert "category" in m, f"Metric missing 'category': {m}"
            assert "description" in m, f"Metric missing 'description': {m}"
            assert "queries" in m, f"Metric missing 'queries': {m}"
            assert len(m["queries"]) >= 1, f"Metric has no queries: {m}"

    def test_names_are_unique(self):
        names = [m["name"] for m in METRICS_CATALOG]
        assert len(names) == len(set(names)), "Duplicate metric names found"

    def test_categories_are_known(self):
        known = {"Cluster Health", "Throughput", "Request Performance", "Broker Resources", "Replication"}
        for m in METRICS_CATALOG:
            assert m["category"] in known, f"Unknown category: {m['category']}"


class TestExtractTopicFromMetricName:
    """Tests for _extract_topic_from_metric_name()."""

    def test_extracts_topic_from_rate1m(self):
        name = "kafka_server_brokertopicmetrics_messagesinpersec_topic_audit_log_topic_rate1m"
        assert _extract_topic_from_metric_name(name) == "audit_log_topic"

    def test_extracts_topic_from_total(self):
        name = "kafka_server_brokertopicmetrics_messagesinpersec_topic_orders_total"
        assert _extract_topic_from_metric_name(name) == "orders"

    def test_returns_none_for_aggregate(self):
        name = "kafka_server_brokertopicmetrics_messagesinpersec_rate1m"
        assert _extract_topic_from_metric_name(name) is None

    def test_returns_none_for_unrelated(self):
        assert _extract_topic_from_metric_name("kafka_server_replicamanager_partitioncount") is None


class TestGetMetricsCatalogSummary:
    """Tests for get_metrics_catalog_summary()."""

    def test_returns_string(self):
        result = get_metrics_catalog_summary()
        assert isinstance(result, str)
        assert "Available Kafka broker metrics" in result

    def test_includes_all_metric_names(self):
        result = get_metrics_catalog_summary()
        for m in METRICS_CATALOG:
            assert m["name"] in result


class TestFetchMetricsFromPrometheus:
    """Tests for fetch_metrics_from_prometheus()."""

    @patch("src.kafka.metrics.httpx.Client")
    def test_fetches_all_metrics(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": {"result": [{"value": [1234567890, "42"]}]}
        }
        mock_resp.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_resp

        results = fetch_metrics_from_prometheus("http://prometheus:9090")
        assert len(results) == len(METRICS_CATALOG)
        assert results[0]["value"] == "42"

    @patch("src.kafka.metrics.httpx.Client")
    def test_fetches_specific_metrics(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": {"result": [{"value": [0, "99"]}]}
        }
        mock_resp.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_resp

        results = fetch_metrics_from_prometheus(
            "http://prom:9090", metric_names=["under_replicated_partitions"],
        )
        assert len(results) == 1
        assert results[0]["name"] == "under_replicated_partitions"
        assert results[0]["value"] == "99"

    @patch("src.kafka.metrics.httpx.Client")
    def test_handles_breakdown_results(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": {
                "result": [
                    {"metric": {"topic": "orders"}, "value": [0, "100"]},
                    {"metric": {"topic": "payments"}, "value": [0, "200"]},
                ]
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_resp

        results = fetch_metrics_from_prometheus(
            "http://prom:9090", metric_names=["messages_in_per_sec_by_topic"],
        )
        assert len(results) == 1
        assert "breakdown" in results[0]
        assert len(results[0]["breakdown"]) == 2

    @patch("src.kafka.metrics.httpx.Client")
    def test_handles_empty_result(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": {"result": []}}
        mock_resp.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_resp

        results = fetch_metrics_from_prometheus(
            "http://prom:9090", metric_names=["under_replicated_partitions"],
        )
        assert len(results) == 1
        assert results[0]["value"] == "no data"

    @patch("src.kafka.metrics.httpx.Client")
    def test_handles_connect_error(self, mock_client_cls):
        mock_client_cls.return_value.__enter__ = MagicMock(
            side_effect=httpx.ConnectError("Connection refused"),
        )
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        results = fetch_metrics_from_prometheus("http://unreachable:9090")
        assert results == []

    @patch("src.kafka.metrics.httpx.Client")
    def test_handles_per_metric_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_client.get.side_effect = Exception("timeout")

        results = fetch_metrics_from_prometheus(
            "http://prom:9090", metric_names=["under_replicated_partitions"],
        )
        assert len(results) == 1
        assert "error" in results[0]["value"]


class TestFormatMetricsForPrompt:
    """Tests for format_metrics_for_prompt()."""

    def test_empty_list(self):
        assert format_metrics_for_prompt([]) == ""

    def test_single_scalar_metric(self):
        metrics = [{
            "name": "under_replicated_partitions",
            "category": "Cluster Health",
            "description": "Under-replicated partitions",
            "value": "0",
        }]
        result = format_metrics_for_prompt(metrics)
        assert "under_replicated_partitions: 0" in result
        assert "Cluster Health" in result

    def test_breakdown_metric(self):
        metrics = [{
            "name": "messages_in_per_sec_by_topic",
            "category": "Throughput",
            "description": "Per-topic messages/sec",
            "breakdown": [
                {"labels": {"topic": "orders"}, "value": "100"},
                {"labels": {"topic": "payments"}, "value": "50"},
            ],
        }]
        result = format_metrics_for_prompt(metrics)
        assert "topic=orders: 100" in result
        assert "topic=payments: 50" in result

    def test_truncates_large_breakdown(self):
        metrics = [{
            "name": "test",
            "category": "Throughput",
            "description": "test",
            "breakdown": [
                {"labels": {"topic": f"t{i}"}, "value": str(i)} for i in range(25)
            ],
        }]
        result = format_metrics_for_prompt(metrics)
        assert "and 5 more" in result
