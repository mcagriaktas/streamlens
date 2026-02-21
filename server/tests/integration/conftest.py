"""Fixtures for integration tests against real Kafka + Schema Registry + Kafka Connect."""
import json
import os
import time

import httpx
import pytest
from confluent_kafka.admin import AdminClient, NewTopic


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
KAFKA_CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")


def _wait_for_kafka(timeout=60):
    """Block until Kafka is reachable."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            metadata = admin.list_topics(timeout=5)
            if metadata is not None:
                return
        except Exception:
            pass
        time.sleep(2)
    pytest.skip("Kafka not reachable — skipping integration tests")


def _wait_for_schema_registry(timeout=60):
    """Block until Schema Registry is reachable."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=5)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(2)
    pytest.skip("Schema Registry not reachable — skipping integration tests")


def _wait_for_kafka_connect(timeout=90):
    """Block until Kafka Connect REST API is reachable."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(f"{KAFKA_CONNECT_URL}/connectors", timeout=5)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(3)
    pytest.skip("Kafka Connect not reachable — skipping integration tests")


@pytest.fixture(scope="session", autouse=True)
def wait_for_services():
    """Wait for Kafka, Schema Registry, and Kafka Connect before running any integration test."""
    _wait_for_kafka()
    _wait_for_schema_registry()
    _wait_for_kafka_connect()


@pytest.fixture(scope="session")
def kafka_admin():
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})


@pytest.fixture(scope="session")
def test_topics(kafka_admin):
    """Create test topics and return their names. Cleaned up after the session."""
    topic_names = ["inttest-orders", "inttest-payments", "inttest-users"]
    new_topics = [NewTopic(t, num_partitions=2, replication_factor=1) for t in topic_names]
    fs = kafka_admin.create_topics(new_topics, request_timeout=10)
    for topic, f in fs.items():
        try:
            f.result()
        except Exception:
            pass  # topic may already exist

    yield topic_names

    # Cleanup
    kafka_admin.delete_topics(topic_names, request_timeout=10)


@pytest.fixture(scope="session")
def test_schemas(test_topics):
    """Register schemas for test topics. Returns a mapping of subject -> schema id."""
    schemas = {}
    with httpx.Client(timeout=10) as client:
        # Same schema for orders-value and payments-value (will get same ID)
        shared_schema = json.dumps({
            "type": "record",
            "name": "Transaction",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "amount", "type": "double"},
            ]
        })
        for topic in ["inttest-orders", "inttest-payments"]:
            subject = f"{topic}-value"
            r = client.post(
                f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
                json={"schema": shared_schema},
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            )
            r.raise_for_status()
            schemas[subject] = r.json()["id"]

        # Different schema for users-value
        user_schema = json.dumps({
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
            ]
        })
        subject = "inttest-users-value"
        r = client.post(
            f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
            json={"schema": user_schema},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        r.raise_for_status()
        schemas[subject] = r.json()["id"]

    yield schemas

    # Cleanup subjects
    with httpx.Client(timeout=10) as client:
        for subject in schemas:
            try:
                client.delete(f"{SCHEMA_REGISTRY_URL}/subjects/{subject}")
                client.delete(f"{SCHEMA_REGISTRY_URL}/subjects/{subject}?permanent=true")
            except Exception:
                pass


@pytest.fixture()
def cluster_config():
    return {
        "id": 99,
        "name": "integration-test",
        "bootstrapServers": KAFKA_BOOTSTRAP,
        "schemaRegistryUrl": SCHEMA_REGISTRY_URL,
        "connectUrl": KAFKA_CONNECT_URL,
    }


@pytest.fixture(scope="session")
def test_connector(test_topics):
    """Deploy a FileStreamSink connector that reads from inttest-orders. Cleaned up after session."""
    connector_name = "inttest-file-sink"
    connector_config = {
        "name": connector_name,
        "config": {
            "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
            "tasks.max": "1",
            "topics": "inttest-orders",
            "file": "/tmp/inttest-orders-sink.txt",
        },
    }

    with httpx.Client(timeout=15) as client:
        r = client.put(
            f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/config",
            json=connector_config["config"],
            headers={"Content-Type": "application/json"},
        )
        r.raise_for_status()

    # Give Connect time to start the task
    time.sleep(3)

    yield connector_name

    # Cleanup
    with httpx.Client(timeout=10) as client:
        try:
            client.delete(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}")
        except Exception:
            pass
