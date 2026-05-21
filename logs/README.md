# Logs Directory

This directory contains execution logs from various data processing tasks.

## 📁 Log Files

### Kafka Streaming Logs
- `streaming_cluster.log` - Kafka streaming in cluster mode (failed due to Kafka connection timeout)
- `streaming_local.log` - Kafka streaming in local mode (failed due to Kafka connection timeout)
- `streaming.log` - Early streaming test logs

## 🔍 Common Issues

**Kafka Connection Timeout**:
```
TimeoutException: Timed out waiting for a node assignment. Call: listTopics
```

**Root Cause**: Unable to resolve Kafka hostname `kafka:9092`

**Workaround**: Use `load_historical_data.py` to load data directly from files instead of Kafka streaming.

## 🗑️ Maintenance

These logs can be safely deleted after troubleshooting is complete.
