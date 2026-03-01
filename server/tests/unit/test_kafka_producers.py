"""Unit tests for server/src/kafka/producers.py."""
from unittest.mock import MagicMock, patch

import httpx
import pytest

import src.kafka.producers as producers_mod
from src.kafka.producers import (
    _parse_active_topics,
    detect_producers_by_offset_change,
    fetch_acl_producers,
    fetch_jmx_producers,
    fetch_prometheus_broker_producers,
    fetch_prometheus_producers,
)


@pytest.fixture(autouse=True)
def clear_offset_baseline():
    """Reset _offset_baseline between tests."""
    producers_mod._offset_baseline.clear()
    yield
    producers_mod._offset_baseline.clear()


def _make_metric(mbean_name: str, count_value: float, attribute: str = "Count"):
    """Create a mock JMX metric object."""
    m = MagicMock()
    m.mBeanName = mbean_name
    m.attribute = attribute
    m.value = count_value
    return m


class TestParseActiveTopics:
    """Tests for _parse_active_topics(metrics)."""

    def test_metric_with_topic_and_count_gt_zero_in_result(self):
        metrics = [
            _make_metric("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=orders", 42.5),
        ]
        result = _parse_active_topics(metrics)
        assert "orders" in result

    def test_metric_with_count_zero_not_in_result(self):
        metrics = [
            _make_metric("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=orders", 0),
        ]
        result = _parse_active_topics(metrics)
        assert "orders" not in result

    def test_internal_topic_filtered_out(self):
        metrics = [
            _make_metric(
                "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=__consumer_offsets", 100
            ),
        ]
        result = _parse_active_topics(metrics)
        assert "__consumer_offsets" not in result

    def test_metric_without_count_attribute_skipped(self):
        metrics = [
            _make_metric("kafka.server:type=BrokerTopicMetrics,topic=orders", 10, attribute="OneMinuteRate"),
        ]
        result = _parse_active_topics(metrics)
        assert "orders" not in result

    def test_multiple_topics_mixed(self):
        metrics = [
            _make_metric("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=orders", 5),
            _make_metric("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=events", 0),
            _make_metric(
                "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=__consumer_offsets",
                100,
            ),
        ]
        result = _parse_active_topics(metrics)
        assert result == {"orders"}

    def test_value_as_string_with_space_parsed(self):
        m = MagicMock()
        m.mBeanName = "kafka.server:...,topic=test"
        m.attribute = "Count"
        m.value = "123.45 ops"
        metrics = [m]
        result = _parse_active_topics(metrics)
        assert "test" in result


class TestFetchJmxProducers:
    """Tests for fetch_jmx_producers(jmx_host, jmx_port)."""

    @patch("jmxquery.JMXQuery", MagicMock())
    @patch("jmxquery.JMXConnection")
    def test_returns_producer_dicts_from_jmx(self, mock_jmx_conn_cls):
        mock_conn = MagicMock()
        mock_jmx_conn_cls.return_value = mock_conn

        m1 = _make_metric("kafka.server:...,topic=orders", 10)
        m2 = _make_metric("kafka.server:...,topic=events", 5)
        mock_conn.query.return_value = [m1, m2]

        result = fetch_jmx_producers("localhost", 9999)
        assert len(result) == 2
        topics = {r["producesTo"][0] for r in result}
        assert "orders" in topics
        assert "events" in topics
        assert all(r["source"] == "jmx" for r in result)

    def test_jmxquery_import_error_returns_empty(self):
        import builtins

        _orig = builtins.__import__

        def custom_import(name, *args, **kwargs):
            if name == "jmxquery":
                raise ImportError("jmxquery not installed")
            return _orig(name, *args, **kwargs)

        with patch.object(builtins, "__import__", custom_import):
            result = fetch_jmx_producers("localhost", 9999)
        assert result == []

    @patch("jmxquery.JMXQuery", MagicMock())
    @patch("jmxquery.JMXConnection")
    def test_exception_returns_empty_list(self, mock_jmx_conn_cls):
        mock_jmx_conn_cls.side_effect = Exception("Connection refused")
        result = fetch_jmx_producers("localhost", 9999)
        assert result == []


class TestFetchAclProducers:
    """Tests for fetch_acl_producers(admin)."""

    @patch("confluent_kafka.admin.AclBindingFilter", MagicMock())
    @patch("confluent_kafka.admin.ResourceType", MagicMock())
    @patch("confluent_kafka.admin.ResourcePatternType", MagicMock())
    @patch("confluent_kafka.admin.AclOperation", MagicMock())
    @patch("confluent_kafka.admin.AclPermissionType", MagicMock())
    def test_returns_grouped_producers_from_write_acls(self):
        mock_admin = MagicMock()
        acl1 = MagicMock()
        acl1.resource_name = "orders"
        acl1.principal = "User:alice"
        acl2 = MagicMock()
        acl2.resource_name = "events"
        acl2.principal = "User:alice"
        acl3 = MagicMock()
        acl3.resource_name = "logs"
        acl3.principal = "ServiceAccount:sa-1"
        mock_result = MagicMock()
        mock_result.result.return_value = [acl1, acl2, acl3]
        mock_admin.describe_acls.return_value = mock_result

        result = fetch_acl_producers(mock_admin)
        assert len(result) == 2
        principals = {r["principal"] for r in result}
        assert "alice" in principals
        assert "sa-1" in principals
        alice_producer = next(r for r in result if r["principal"] == "alice")
        assert set(alice_producer["producesTo"]) == {"orders", "events"}

    @patch("confluent_kafka.admin.AclBindingFilter", MagicMock())
    @patch("confluent_kafka.admin.ResourceType", MagicMock())
    @patch("confluent_kafka.admin.ResourcePatternType", MagicMock())
    @patch("confluent_kafka.admin.AclOperation", MagicMock())
    @patch("confluent_kafka.admin.AclPermissionType", MagicMock())
    def test_no_acls_returns_empty_list(self):
        mock_admin = MagicMock()
        mock_result = MagicMock()
        mock_result.result.return_value = []
        mock_admin.describe_acls.return_value = mock_result
        result = fetch_acl_producers(mock_admin)
        assert result == []

    @patch("confluent_kafka.admin.AclBindingFilter", MagicMock())
    @patch("confluent_kafka.admin.ResourceType", MagicMock())
    @patch("confluent_kafka.admin.ResourcePatternType", MagicMock())
    @patch("confluent_kafka.admin.AclOperation", MagicMock())
    @patch("confluent_kafka.admin.AclPermissionType", MagicMock())
    def test_filters_internal_topics(self):
        mock_admin = MagicMock()
        acl = MagicMock()
        acl.resource_name = "__consumer_offsets"
        acl.principal = "User:alice"
        mock_result = MagicMock()
        mock_result.result.return_value = [acl]
        mock_admin.describe_acls.return_value = mock_result
        result = fetch_acl_producers(mock_admin)
        assert result == []

    def test_exception_returns_empty_list(self):
        mock_admin = MagicMock()
        mock_admin.describe_acls.side_effect = Exception("ACL not enabled")
        result = fetch_acl_producers(mock_admin)
        assert result == []


class TestFetchPrometheusProducers:
    """Tests for fetch_prometheus_producers(prometheus_url)."""

    def _mock_response(self, results: list, status_code: int = 200):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = status_code
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"status": "success", "data": {"resultType": "vector", "result": results}}
        return resp

    @patch("src.kafka.producers.httpx.Client")
    def test_returns_per_client_per_topic_producers(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = self._mock_response([
            {"metric": {"client_id": "orders-app", "topic": "orders"}, "value": [1700000000, "500"]},
            {"metric": {"client_id": "orders-app", "topic": "events"}, "value": [1700000000, "300"]},
            {"metric": {"client_id": "payments-svc", "topic": "payments"}, "value": [1700000000, "100"]},
        ])

        result = fetch_prometheus_producers("http://prometheus:9090")

        assert len(result) == 2
        p1 = next(r for r in result if r["clientId"] == "orders-app")
        assert set(p1["producesTo"]) == {"orders", "events"}
        assert p1["source"] == "prometheus"
        p2 = next(r for r in result if r["clientId"] == "payments-svc")
        assert p2["producesTo"] == ["payments"]

    @patch("src.kafka.producers.httpx.Client")
    def test_empty_results_returns_empty(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = self._mock_response([])

        result = fetch_prometheus_producers("http://prometheus:9090")
        assert result == []

    @patch("src.kafka.producers.httpx.Client")
    def test_filters_internal_topics(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = self._mock_response([
            {"metric": {"client_id": "app", "topic": "__consumer_offsets"}, "value": [0, "100"]},
            {"metric": {"client_id": "app", "topic": "orders"}, "value": [0, "50"]},
        ])

        result = fetch_prometheus_producers("http://prometheus:9090")
        assert len(result) == 1
        assert result[0]["producesTo"] == ["orders"]

    @patch("src.kafka.producers.httpx.Client")
    def test_zero_value_metric_skipped(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = self._mock_response([
            {"metric": {"client_id": "idle-app", "topic": "orders"}, "value": [0, "0"]},
        ])

        result = fetch_prometheus_producers("http://prometheus:9090")
        assert result == []

    @patch("src.kafka.producers.httpx.Client")
    def test_connect_error_returns_empty(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get.side_effect = httpx.ConnectError("Connection refused")

        result = fetch_prometheus_producers("http://prometheus:9090")
        assert result == []

    @patch("src.kafka.producers.httpx.Client")
    def test_http_error_returns_empty(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 500
        resp.raise_for_status.side_effect = httpx.HTTPStatusError("error", request=MagicMock(), response=resp)
        mock_client.get.return_value = resp

        result = fetch_prometheus_producers("http://prometheus:9090")
        assert result == []

    @patch("src.kafka.producers.httpx.Client")
    def test_missing_client_id_skipped(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = self._mock_response([
            {"metric": {"topic": "orders"}, "value": [0, "100"]},
        ])

        result = fetch_prometheus_producers("http://prometheus:9090")
        assert result == []


class TestFetchPrometheusBrokerProducers:
    """Tests for fetch_prometheus_broker_producers(prometheus_url)."""

    @patch("src.kafka.producers.httpx.Client")
    def test_detects_active_topics(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": {
                "result": [
                    {
                        "metric": {"__name__": "kafka_server_brokertopicmetrics_messagesinpersec_topic_orders_rate1m"},
                        "value": [0, "15.5"],
                    },
                    {
                        "metric": {"__name__": "kafka_server_brokertopicmetrics_messagesinpersec_topic_payments_rate1m"},
                        "value": [0, "8.2"],
                    },
                ]
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_resp

        result = fetch_prometheus_broker_producers("http://prom:9090")
        assert len(result) == 2
        assert result[0]["id"] == "prometheus-broker:active-producer:orders"
        assert result[0]["source"] == "prometheus-broker"
        assert result[0]["producesTo"] == ["orders"]
        assert result[1]["id"] == "prometheus-broker:active-producer:payments"

    @patch("src.kafka.producers.httpx.Client")
    def test_filters_internal_topics(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": {
                "result": [
                    {
                        "metric": {"__name__": "kafka_server_brokertopicmetrics_messagesinpersec_topic___consumer_offsets_rate1m"},
                        "value": [0, "5.0"],
                    },
                    {
                        "metric": {"__name__": "kafka_server_brokertopicmetrics_messagesinpersec_topic_connect_configs_rate1m"},
                        "value": [0, "1.0"],
                    },
                    {
                        "metric": {"__name__": "kafka_server_brokertopicmetrics_messagesinpersec_topic_orders_rate1m"},
                        "value": [0, "10.0"],
                    },
                ]
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_resp

        result = fetch_prometheus_broker_producers("http://prom:9090")
        assert len(result) == 1
        assert result[0]["producesTo"] == ["orders"]

    @patch("src.kafka.producers.httpx.Client")
    def test_skips_zero_rate(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": {
                "result": [
                    {
                        "metric": {"__name__": "kafka_server_brokertopicmetrics_messagesinpersec_topic_orders_rate1m"},
                        "value": [0, "0"],
                    },
                ]
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_resp

        result = fetch_prometheus_broker_producers("http://prom:9090")
        assert result == []

    @patch("src.kafka.producers.httpx.Client")
    def test_connect_error_returns_empty(self, mock_client_cls):
        mock_client_cls.return_value.__enter__ = MagicMock(
            side_effect=httpx.ConnectError("Connection refused"),
        )
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        result = fetch_prometheus_broker_producers("http://unreachable:9090")
        assert result == []

    @patch("src.kafka.producers.httpx.Client")
    def test_works_with_topic_label(self, mock_client_cls):
        """Fallback query format uses topic as a label instead of embedded in name."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp_empty = MagicMock()
        mock_resp_empty.json.return_value = {"data": {"result": []}}
        mock_resp_empty.raise_for_status = MagicMock()

        mock_resp_data = MagicMock()
        mock_resp_data.json.return_value = {
            "data": {
                "result": [
                    {"metric": {"topic": "orders"}, "value": [0, "5.0"]},
                ]
            }
        }
        mock_resp_data.raise_for_status = MagicMock()

        mock_client.get.side_effect = [mock_resp_empty, mock_resp_data]

        result = fetch_prometheus_broker_producers("http://prom:9090")
        assert len(result) == 1
        assert result[0]["producesTo"] == ["orders"]


class TestDetectProducersByOffsetChange:
    """Tests for detect_producers_by_offset_change(cluster_id, client_cfg, topics)."""

    @patch("src.kafka.producers.Consumer")
    def test_first_call_returns_empty_stores_baseline(self, mock_consumer_cls):
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        mock_consumer.get_watermark_offsets.return_value = (0, 100)

        topics = [{"name": "orders", "partitions": 2}]
        result = detect_producers_by_offset_change(1, {"bootstrap.servers": "localhost:9092"}, topics)

        assert result == []
        assert 1 in producers_mod._offset_baseline

    @patch("src.kafka.producers.Consumer")
    def test_second_call_with_increased_offsets_returns_producers(self, mock_consumer_cls):
        from confluent_kafka import TopicPartition

        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        def watermark_first(tp, **kwargs):
            if isinstance(tp, TopicPartition) and tp.topic == "orders":
                return (0, 100) if tp.partition == 0 else (0, 200)
            return (0, 50)

        def watermark_second(tp, **kwargs):
            if isinstance(tp, TopicPartition) and tp.topic == "orders":
                return (0, 150) if tp.partition == 0 else (0, 250)
            return (0, 50)

        mock_consumer.get_watermark_offsets.side_effect = watermark_first

        topics = [{"name": "orders", "partitions": 2}]
        client_cfg = {"bootstrap.servers": "localhost:9092"}

        first = detect_producers_by_offset_change(2, client_cfg, topics)
        assert first == []

        mock_consumer.get_watermark_offsets.side_effect = watermark_second
        second = detect_producers_by_offset_change(2, client_cfg, topics)
        assert len(second) >= 1
        assert any("orders" in p["producesTo"] for p in second)

    @patch("src.kafka.producers.Consumer")
    def test_empty_topics_returns_empty(self, mock_consumer_cls):
        result = detect_producers_by_offset_change(3, {"bootstrap.servers": "localhost:9092"}, [])
        assert result == []
