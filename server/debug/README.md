# Debug & Utility Scripts

Helper scripts for troubleshooting connections and inspecting cluster state.

## Scripts

| Script | Purpose |
|--------|---------|
| `configure_jmx.py` | Configure JMX settings for a cluster |
| `debug_consumer_groups.py` | Debug consumer group visibility |
| `debug_kafka_connection.py` | Debug Kafka connection issues |
| `debug_member_details.py` | Inspect consumer group member details |
| `debug_jmx_structure.py` | Debug JMX metric structure |
| `check_producer_detection.py` | Explore producer detection limitations |
| `explore_jmx_clients.py` | Explore available JMX metrics |
| `test_jmx_connection.py` | Test JMX connectivity |
| `test_topic_config.py` | Test topic configuration retrieval |
| `test_connectors.py` | Test Kafka Connect connector retrieval |
| `test_connector_masking.py` | Test sensitive config masking |

## Usage

```bash
cd server
uv run python debug/debug_consumer_groups.py
uv run python debug/configure_jmx.py 1 localhost 9999
```
