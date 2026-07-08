# workers-buffer-tuning

**Question:** best `Kafka:WorkersCount` × `Kafka:BufferSize` for **CPU-bound** processing under the
FreeWorker strategy (keyless workload).

## Design

- Keyless messages; each incurs `Benchmark:WorkMicros` (~700 µs) of synthetic CPU work in the handler
  (busy-spin), no output produce — isolates the worker/buffer effect.
- Grid: workers {4, 16, 32} × buffer {10, 100, 1000} = 9 runs, backlog-drain.
- Captures throughput, avg processing latency, CPU cores, peak memory, GC.

Finding: throughput scales with workers up to ≈ the host core count, then flattens while latency rises
(CPU oversubscription); buffer size is ~noise for a keyless/CPU-bound steady load.

## Run

```bash
cd Processor && docker compose up -d zookeeper broker && cd ..
dotnet build Processor/Processor.csproj -c Release

python3 loadtests/workers-buffer-tuning/gen_load.py     # -> .work/load_keyless.txt
loadtests/workers-buffer-tuning/sweep.sh                # 9 runs -> .work/sweep_*.json
python3 loadtests/workers-buffer-tuning/make_report.py  # -> .work/workers_report.html
node    loadtests/workers-buffer-tuning/pdf.js "$PWD/WORKERS_BUFFER_TUNING.pdf" .work/workers_report.html
```

Env overrides: `N`, `WORKERS`/`BUFFERS` (edit `sweep.sh`), `WORK_DIR`, `PROJECT_ROOT`.
PDF rendering needs Playwright Chromium (`npm i playwright && npx playwright install chromium`).

## Files

| File | Purpose |
|------|---------|
| `gen_load.py` | keyless load + warmup into `.work/` |
| `measure_run.py` | one config: produce → drain → throughput/latency/CPU/mem/GC |
| `sweep.sh` | app lifecycle + `measure_run.py` across the grid |
| `make_report.py` | `.work/sweep_*.json` → `.work/workers_report.html` |
| `pdf.js` | HTML → PDF (Playwright); args: `<out.pdf> <input.html>` |
