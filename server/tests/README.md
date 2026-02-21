# Test Scripts

Scripts for verifying Kafka functionality.

## Scripts

| Script | Purpose |
|--------|---------|
| `test_jmx.py` | Test JMX connection and producer detection |
| `test_consumer_query.py` | Test consumer group detection |
| `test_list_groups.py` | Test consumer group listing |

## Usage

```bash
cd server
uv run python tests/test_jmx.py
uv run python tests/test_consumer_query.py
```
