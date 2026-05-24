# Streaming Micro-Batch Processing

This namespace is reserved for simulated streaming and incremental micro-batch
processing.

HDFS layout:

- `hdfs://node1:9000/lake/stream/bronze/bronze_streaming`
- `hdfs://node1:9000/lake/stream/silver`
- `hdfs://node1:9000/lake/stream/gold`
- `hdfs://node1:9000/lake/control`

Kafka ingestion now defaults to the streaming Bronze table. Silver and Gold
incremental jobs will use ingest-time watermarks from `/lake/control`.

The API can read this namespace with:

```bash
ENERGY_DATA_MODE=stream
HDFS_LAKE_PATH=hdfs://node1:9000/lake/stream
```
