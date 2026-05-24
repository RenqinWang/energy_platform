# Full Batch Processing

This namespace preserves the existing full-batch result set.

HDFS layout:

- `hdfs://node1:9000/lake/full/silver`
- `hdfs://node1:9000/lake/full/gold`

The API reads this namespace by default with:

```bash
ENERGY_DATA_MODE=full
HDFS_LAKE_PATH=hdfs://node1:9000/lake/full
```

The current full-batch scripts remain in `data-processing/silver-layer` and
`data-processing/gold-layer`; they will be parameterized before being moved or
wrapped here.
