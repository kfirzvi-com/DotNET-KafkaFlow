#!/usr/bin/env python3
"""Fetch /metrics and print aggregate JSON used by the load test."""
import json, sys, urllib.request

URL = "http://localhost:8080/metrics"

def fetch():
    with urllib.request.urlopen(URL, timeout=5) as r:
        return r.read().decode()

def main():
    text = fetch()
    agg = {
        "processed": 0.0, "filtered": 0.0, "dead_lettered": 0.0, "dropped": 0.0,
        "proc_sum": 0.0, "proc_count": 0.0,
        "redis_ops": 0.0, "redis_sum": 0.0, "redis_count": 0.0,
    }
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        # split "name{labels} value ts" -> name part and value
        try:
            metric, rest = line.split(" ", 1) if "{" not in line else (line.split("}")[0] + "}", line.split("} ",1)[1])
        except ValueError:
            continue
        parts = rest.split()
        if not parts:
            continue
        try:
            val = float(parts[0])
        except ValueError:
            continue
        name = metric.split("{")[0]
        if name == "messages_processed_total": agg["processed"] += val
        elif name == "messages_filtered_total": agg["filtered"] += val
        elif name == "messages_dead_lettered_total": agg["dead_lettered"] += val
        elif name == "messages_dropped_total": agg["dropped"] += val
        elif name == "messages_processing_duration_milliseconds_sum": agg["proc_sum"] += val
        elif name == "messages_processing_duration_milliseconds_count": agg["proc_count"] += val
        elif name == "redis_operations_total": agg["redis_ops"] += val
        elif name == "redis_operation_duration_milliseconds_sum": agg["redis_sum"] += val
        elif name == "redis_operation_duration_milliseconds_count": agg["redis_count"] += val
    agg["handled"] = agg["processed"] + agg["filtered"] + agg["dead_lettered"] + agg["dropped"]
    print(json.dumps(agg))

if __name__ == "__main__":
    main()
