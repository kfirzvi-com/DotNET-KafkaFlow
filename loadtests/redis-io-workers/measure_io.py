#!/usr/bin/env python3
"""Measure one I/O-bound config (Redis GET per message). App must be running + idle.
Usage: measure_io.py <label> <N> <workers> <buffer>
Env: WORK_DIR (default ./.work), METRICS_URL (default http://localhost:8080/metrics),
     BROKER_CONTAINER (default broker), TOPIC (default input-topic)
"""
import json, os, subprocess, sys, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.environ.get("WORK_DIR", os.path.join(HERE, ".work"))
URL = os.environ.get("METRICS_URL", "http://localhost:8080/metrics")
BROKER = os.environ.get("BROKER_CONTAINER", "broker")
TOPIC = os.environ.get("TOPIC", "input-topic")

def produce_keyed(path):
    with open(path, "rb") as f:
        subprocess.run(
            ["docker","exec","-i",BROKER,"kafka-console-producer",
             "--bootstrap-server","broker:9092","--topic",TOPIC,
             "--property","parse.key=true","--property","key.separator=|"],
            stdin=f, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def fetch():
    last = None
    for _ in range(5):
        try:
            with urllib.request.urlopen(URL, timeout=10) as r:
                return r.read().decode()
        except Exception as e:
            last = e; time.sleep(1)
    raise last

def snap():
    text = fetch()
    a = {k: 0.0 for k in ("handled","proc_sum","proc_count","cpu","gc","ws","redis_ops","redis_sum","redis_count")}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        name = line.split("{")[0].split(" ")[0]
        try:
            v = float(line.split("}")[-1].split()[0] if "{" in line else line.split()[1])
        except (ValueError, IndexError):
            continue
        if name in ("messages_processed_total","messages_filtered_total","messages_dead_lettered_total","messages_dropped_total"):
            a["handled"] += v
        elif name == "messages_processing_duration_milliseconds_sum": a["proc_sum"] += v
        elif name == "messages_processing_duration_milliseconds_count": a["proc_count"] += v
        elif name == "dotnet_process_cpu_time_seconds_total": a["cpu"] += v
        elif name == "dotnet_gc_collections_total": a["gc"] += v
        elif name == "dotnet_process_memory_working_set_bytes": a["ws"] = max(a["ws"], v)
        elif name == "redis_operations_total": a["redis_ops"] += v
        elif name == "redis_operation_duration_milliseconds_sum": a["redis_sum"] += v
        elif name == "redis_operation_duration_milliseconds_count": a["redis_count"] += v
    return a

def main():
    label, N, workers, buf = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])

    b0 = snap()["handled"]
    produce_keyed(os.path.join(WORK, "io_warmup.txt"))
    dl = time.time() + 60
    while time.time() < dl:
        if snap()["handled"] >= b0 + 5000: break
        time.sleep(0.5)
    time.sleep(2)

    before = snap()
    target = before["handled"] + N
    t0 = time.time()
    produce_keyed(os.path.join(WORK, "io_load.txt"))

    ws_peak = before["ws"]; cur = before
    dl = time.time() + 600
    while time.time() < dl:
        cur = snap()
        ws_peak = max(ws_peak, cur["ws"])
        if cur["handled"] >= target: break
        time.sleep(0.3)
    t1 = time.time()

    dur = t1 - t0
    handled = cur["handled"] - before["handled"]  # actual processed (loop overshoots N between polls)
    pc = cur["proc_count"] - before["proc_count"]
    rc = cur["redis_count"] - before["redis_count"]
    res = {
        "label": label, "workers": workers, "buffer": buf, "messages": round(handled),
        "seconds": round(dur, 2),
        "throughput_msg_s": round(handled / dur, 1) if dur > 0 else 0,
        "avg_proc_ms": round((cur["proc_sum"] - before["proc_sum"]) / pc, 4) if pc > 0 else None,
        "redis_ops": round(cur["redis_ops"] - before["redis_ops"]),
        "redis_ops_per_msg": round((cur["redis_ops"] - before["redis_ops"]) / handled, 3) if handled > 0 else None,
        "avg_redis_ms": round((cur["redis_sum"] - before["redis_sum"]) / rc, 4) if rc > 0 else None,
        "cpu_cores": round((cur["cpu"] - before["cpu"]) / dur, 2) if dur > 0 else None,
        "gc_per_s": round((cur["gc"] - before["gc"]) / dur, 2) if dur > 0 else None,
        "mem_peak_mb": round(ws_peak / 1048576, 1),
        "completed": cur["handled"] >= target,
    }
    os.makedirs(WORK, exist_ok=True)
    with open(os.path.join(WORK, f"io_{label}.json"), "w") as f:
        json.dump(res, f, indent=2)
    print(json.dumps(res))

if __name__ == "__main__":
    main()
