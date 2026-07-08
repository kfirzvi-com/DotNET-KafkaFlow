#!/usr/bin/env python3
"""Measure one (workers,buffer) config. App must already be running + idle.

Flow: warmup produce -> snapshot -> produce N keyless -> poll to drain N
      -> compute throughput, latency percentiles, memory peak, cpu, gc.
Usage: measure_run.py <label> <N> <workers> <buffer> <workMicros>
"""
import json, subprocess, sys, time, urllib.request

import os
SP = os.environ.get("WORK_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), ".work")
os.makedirs(SP, exist_ok=True)
URL = "http://localhost:8080/metrics"

def produce(path):
    with open(path, "rb") as f:
        subprocess.run(
            ["docker","exec","-i","broker","kafka-console-producer",
             "--bootstrap-server","broker:9092","--topic","input-topic"],
            stdin=f, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def fetch():
    last = None
    for _ in range(5):
        try:
            with urllib.request.urlopen(URL, timeout=10) as r:
                return r.read().decode()
        except Exception as e:  # transient timeout during JIT/GC at startup
            last = e
            time.sleep(1)
    raise last

def snap():
    text = fetch()
    a = {k: 0.0 for k in ("handled","proc_sum","proc_count","cpu","gc","ws")}
    buckets = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        name = line.split("{")[0].split(" ")[0]
        try:
            v = float(line.split("}")[-1].split()[0] if "{" in line else line.split()[1])
        except (ValueError, IndexError):
            continue
        if name in ("messages_processed_total","messages_filtered_total",
                    "messages_dead_lettered_total","messages_dropped_total"):
            a["handled"] += v
        elif name == "messages_processing_duration_milliseconds_sum": a["proc_sum"] += v
        elif name == "messages_processing_duration_milliseconds_count": a["proc_count"] += v
        elif name == "dotnet_process_cpu_time_seconds_total": a["cpu"] += v
        elif name == "dotnet_gc_collections_total": a["gc"] += v
        elif name == "dotnet_process_memory_working_set_bytes": a["ws"] = max(a["ws"], v)
        elif name == "messages_processing_duration_milliseconds_bucket":
            le = line.split('le="')[1].split('"')[0]
            le = float("inf") if le == "+Inf" else float(le)
            buckets[le] = buckets.get(le, 0.0) + v
    a["buckets"] = buckets
    return a

def quantile(before_b, after_b, total, q):
    les = sorted(after_b.keys())
    delta = [(le, after_b.get(le,0) - before_b.get(le,0)) for le in les]
    target = q * total
    prev_le, prev_cum = 0.0, 0.0
    for le, cum in delta:
        if cum >= target:
            if le == float("inf"):
                return prev_le
            # linear interpolation within the bucket
            frac = (target - prev_cum) / (cum - prev_cum) if cum > prev_cum else 0
            return prev_le + frac * (le - prev_le)
        prev_le, prev_cum = le, cum
    return prev_le

def main():
    label, N, workers, buf, work = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])

    # Warm up JIT + consumer, then wait until drained.
    b0 = snap()["handled"]
    produce(f"{SP}/warmup.txt")
    dl = time.time() + 60
    while time.time() < dl:
        if snap()["handled"] >= b0 + 5000:
            break
        time.sleep(0.5)
    time.sleep(2)

    before = snap()
    target = before["handled"] + N
    t0 = time.time()
    produce(f"{SP}/load_keyless.txt")

    ws_peak = before["ws"]
    cur = before
    dl = time.time() + 600
    while time.time() < dl:
        cur = snap()
        ws_peak = max(ws_peak, cur["ws"])
        if cur["handled"] >= target:
            break
        time.sleep(0.3)
    t1 = time.time()

    dur = t1 - t0
    pc = cur["proc_count"] - before["proc_count"]
    res = {
        "label": label, "workers": workers, "buffer": buf, "work_micros": work,
        "messages": N, "seconds": round(dur, 2),
        "throughput_msg_s": round(N / dur, 1) if dur > 0 else 0,
        "avg_proc_ms": round((cur["proc_sum"] - before["proc_sum"]) / pc, 4) if pc > 0 else None,
        "p50_ms": round(quantile(before["buckets"], cur["buckets"], pc, 0.50), 3),
        "p95_ms": round(quantile(before["buckets"], cur["buckets"], pc, 0.95), 3),
        "p99_ms": round(quantile(before["buckets"], cur["buckets"], pc, 0.99), 3),
        "cpu_cores": round((cur["cpu"] - before["cpu"]) / dur, 2) if dur > 0 else None,
        "gc_per_s": round((cur["gc"] - before["gc"]) / dur, 2) if dur > 0 else None,
        "mem_peak_mb": round(ws_peak / 1048576, 1),
        "buffer_capacity_total": workers * buf,
        "completed": cur["handled"] >= target,
    }
    with open(f"{SP}/sweep_{label}.json", "w") as f:
        json.dump(res, f, indent=2)
    print(json.dumps(res))

if __name__ == "__main__":
    main()
