#!/usr/bin/env python3
"""Run one load-test iteration and save result_<label>.json.

Usage: runload.py <label> [N]
Assumes the processor app is already running in the desired cache mode.
"""
import json, subprocess, sys, time, urllib.request

import os
SP = os.environ.get("WORK_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), ".work")
os.makedirs(SP, exist_ok=True)
URL = "http://localhost:8080/metrics"

def snap():
    with urllib.request.urlopen(URL, timeout=5) as r:
        text = r.read().decode()
    agg = {k: 0.0 for k in ("processed","filtered","dead_lettered","dropped",
                            "proc_sum","proc_count","redis_ops","redis_sum","redis_count")}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        name = line.split("{")[0].split(" ")[0]
        val = line.split("}")[-1].split() if "{" in line else line.split()[1:]
        try:
            v = float(val[0])
        except (ValueError, IndexError):
            continue
        if name == "messages_processed_total": agg["processed"] += v
        elif name == "messages_filtered_total": agg["filtered"] += v
        elif name == "messages_dead_lettered_total": agg["dead_lettered"] += v
        elif name == "messages_dropped_total": agg["dropped"] += v
        elif name == "messages_processing_duration_milliseconds_sum": agg["proc_sum"] += v
        elif name == "messages_processing_duration_milliseconds_count": agg["proc_count"] += v
        elif name == "redis_operations_total": agg["redis_ops"] += v
        elif name == "redis_operation_duration_milliseconds_sum": agg["redis_sum"] += v
        elif name == "redis_operation_duration_milliseconds_count": agg["redis_count"] += v
    agg["handled"] = agg["processed"] + agg["filtered"] + agg["dead_lettered"] + agg["dropped"]
    return agg

def main():
    label = sys.argv[1]
    N = int(sys.argv[2]) if len(sys.argv) > 2 else 50000

    before = snap()
    target = before["handled"] + N
    t0 = time.time()

    # Produce the load as fast as the console producer allows.
    with open(f"{SP}/load.txt", "rb") as f:
        subprocess.run(
            ["docker","exec","-i","broker","kafka-console-producer",
             "--bootstrap-server","broker:9092","--topic","input-topic",
             "--property","parse.key=true","--property","key.separator=|"],
            stdin=f, check=True,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    produced_at = time.time()

    deadline = time.time() + 300
    cur = before
    while time.time() < deadline:
        cur = snap()
        if cur["handled"] >= target:
            break
        time.sleep(0.5)
    t1 = time.time()

    dur = t1 - t0
    d = lambda k: cur[k] - before[k]
    proc_c = d("proc_count"); redis_c = d("redis_count")
    res = {
        "label": label,
        "messages": N,
        "seconds": round(dur, 2),
        "produce_seconds": round(produced_at - t0, 2),
        "throughput_msg_s": round(N / dur, 1) if dur > 0 else 0,
        "handled_delta": round(d("handled")),
        "processed_delta": round(d("processed")),
        "avg_proc_latency_ms": round(d("proc_sum") / proc_c, 4) if proc_c > 0 else None,
        "redis_ops": round(d("redis_ops")),
        "redis_ops_per_msg": round(d("redis_ops") / N, 4),
        "avg_redis_latency_ms": round(d("redis_sum") / redis_c, 4) if redis_c > 0 else None,
        "completed": cur["handled"] >= target,
    }
    with open(f"{SP}/result_{label}.json", "w") as f:
        json.dump(res, f, indent=2)
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    main()
