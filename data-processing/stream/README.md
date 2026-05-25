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

Typical demo flow:

```bash
# 1. Start Kafka -> stream Bronze.
./scripts/start-kafka-to-bronze.sh

# 2. Run one micro-batch, or keep refreshing every 5 minutes.
./scripts/run-stream-microbatch.sh once
./scripts/run-stream-microbatch.sh loop

# 3. Serve full and stream API instances side by side.
./scripts/start-backend-mode.sh full
./scripts/start-backend-mode.sh stream
```

Frontend mode switch:

- Full mode calls `http://<frontend-host>:8001/api`.
- Stream mode calls `http://<frontend-host>:8002/api`.
