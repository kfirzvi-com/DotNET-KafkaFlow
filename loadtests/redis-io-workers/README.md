# redis-io-workers

**Question:** for an **I/O-bound** workload — a Redis `GET` of the data-type setting on *every*
message (`DataTypeSettings:UseCache=false`) — how does **worker count** affect throughput, and how
does that differ from CPU-bound work?

## Design

- Settings stored inactive under `datatypesettings:type-0..49`, so each keyed message triggers one
  Redis `GET` in the filter path, then filters out (no output produce) — isolating the Redis I/O.
- Messages are **keyed** with `type-0..49` (the key supplies the data-type id).
- Sweep `Kafka__WorkersCount` ∈ {4, 16, 32, 64, 128} at fixed buffer 100, `UseCache=false`,
  `WorkMicros=0`.
- Backlog-drain: produce N messages, measure drain time → throughput. Capture CPU, Redis op rate,
  Redis latency, memory.

Expected contrast with `workers-buffer-tuning` (CPU-bound): because workers **await** the network,
throughput should keep rising past the core count while CPU stays low.

## Run

```bash
# 0. prerequisites: docker stack up, app built in Release
cd Processor && docker compose up -d zookeeper broker redis && cd ..
dotnet build Processor/Processor.csproj -c Release

# 1. seed inactive settings + generate keyed load files (into .work/)
loadtests/redis-io-workers/seed_redis.sh
python3 loadtests/redis-io-workers/gen_load.py

# 2. run the sweep (starts/stops the app per config)
loadtests/redis-io-workers/sweep.sh

# 3. build the report + PDF
python3 loadtests/redis-io-workers/make_report.py
node    loadtests/redis-io-workers/render_pdf.js
```

Outputs: `.work/io_w*.json` (per-config metrics), `.work/report.html`, and `REDIS_IO_WORKERS.pdf`
in the repo root.

## Files

| File | Purpose |
|------|---------|
| `seed_redis.sh` | `SET datatypesettings:type-{0..49}` inactive |
| `gen_load.py` | keyed load + warmup files into `.work/` |
| `measure_io.py` | one config: produce → drain → throughput/CPU/Redis/memory |
| `sweep.sh` | app lifecycle + `measure_io.py` across worker counts |
| `make_report.py` | `.work/io_*.json` → `.work/report.html` |
| `render_pdf.js` | `report.html` → `REDIS_IO_WORKERS.pdf` (Playwright) |
