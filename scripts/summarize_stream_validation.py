#!/usr/bin/env python3
"""Summarize snapshots and logs from one stream validation trial."""

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional


def load_snapshots(path: Path) -> List[Dict[str, Any]]:
    snapshots = []
    for file_path in sorted(path.glob("snapshots/*.json")):
        try:
            snapshots.append(json.loads(file_path.read_text(encoding="utf-8")))
        except Exception:
            continue
    return snapshots


def parse_time(value: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(value[:19])
    except Exception:
        return None


def metric_delta(snapshots: List[Dict[str, Any]], path: List[str]) -> Optional[float]:
    values = []
    for snapshot in snapshots:
        current: Any = snapshot
        for key in path:
            if not isinstance(current, dict) or key not in current:
                current = None
                break
            current = current[key]
        if isinstance(current, (int, float)):
            values.append(float(current))
    if len(values) < 2:
        return None
    return values[-1] - values[0]


def average_node_cpu(snapshots: List[Dict[str, Any]], node: str) -> Optional[float]:
    values = []
    for snapshot in snapshots:
        value = snapshot.get("nodes", {}).get(node, {}).get("cpu_percent")
        if isinstance(value, (int, float)):
            values.append(float(value))
    return round(mean(values), 2) if values else None


def average_node_mem(snapshots: List[Dict[str, Any]], node: str) -> Optional[float]:
    values = []
    for snapshot in snapshots:
        value = snapshot.get("nodes", {}).get(node, {}).get("mem_used_percent")
        if isinstance(value, (int, float)):
            values.append(float(value))
    return round(mean(values), 2) if values else None


def parse_microbatch_log(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"exists": False}

    starts: List[datetime] = []
    finishes: List[datetime] = []
    rows: List[int] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        match = re.search(r"Stream micro-batch started at ([0-9:-]+ [0-9:]+)", line)
        if match:
            starts.append(datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S"))
        match = re.search(r"Stream micro-batch finished at ([0-9:-]+ [0-9:]+)", line)
        if match:
            finishes.append(datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S"))
        match = re.search(r"Incremental batch rows: ([0-9]+)", line)
        if match:
            rows.append(int(match.group(1)))

    durations = []
    for start, finish in zip(starts, finishes):
        if finish >= start:
            durations.append((finish - start).total_seconds())

    return {
        "exists": True,
        "started_batches": len(starts),
        "finished_batches": len(finishes),
        "incremental_batch_rows": rows,
        "total_incremental_rows_logged": sum(rows),
        "avg_batch_duration_sec": round(mean(durations), 2) if durations else None,
        "max_batch_duration_sec": round(max(durations), 2) if durations else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("trial_dir")
    args = parser.parse_args()

    trial_dir = Path(args.trial_dir)
    snapshots = load_snapshots(trial_dir)
    if not snapshots:
        raise SystemExit(f"No snapshots found under {trial_dir}/snapshots")

    first = snapshots[0]
    last = snapshots[-1]
    first_ts = parse_time(first["timestamp"])
    last_ts = parse_time(last["timestamp"])
    elapsed_sec = (last_ts - first_ts).total_seconds() if first_ts and last_ts else None

    producer_delta = metric_delta(snapshots, ["producer", "messages_sent"])
    kafka_offset_delta = metric_delta(snapshots, ["kafka", "latest_offsets_total"])

    summary = {
        "trial_dir": str(trial_dir),
        "mode": first.get("mode"),
        "start_timestamp": first.get("timestamp"),
        "end_timestamp": last.get("timestamp"),
        "elapsed_sec": elapsed_sec,
        "snapshot_count": len(snapshots),
        "producer_messages_delta": producer_delta,
        "producer_messages_per_sec": round(producer_delta / elapsed_sec, 3) if producer_delta is not None and elapsed_sec else None,
        "kafka_offsets_delta": kafka_offset_delta,
        "kafka_offsets_per_sec": round(kafka_offset_delta / elapsed_sec, 3) if kafka_offset_delta is not None and elapsed_sec else None,
        "avg_cpu_percent": {
            "node1": average_node_cpu(snapshots, "node1"),
            "node2": average_node_cpu(snapshots, "node2"),
            "node3": average_node_cpu(snapshots, "node3"),
        },
        "avg_mem_used_percent": {
            "node1": average_node_mem(snapshots, "node1"),
            "node2": average_node_mem(snapshots, "node2"),
            "node3": average_node_mem(snapshots, "node3"),
        },
        "microbatch": parse_microbatch_log(trial_dir / "stream_microbatch_loop.log"),
        "final_lake": last.get("lake", {}),
    }

    output_path = trial_dir / "summary.json"
    output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(str(output_path))


if __name__ == "__main__":
    main()
