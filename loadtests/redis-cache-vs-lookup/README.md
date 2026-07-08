# redis-cache-vs-lookup

**Question:** what does the in-memory settings cache save versus a Redis lookup on every message
(`DataTypeSettings:UseCache` on vs. off)?

## Design

- Keyed messages (`type-0..49`, seeded **active**) so each is processed and, when uncached, does one
  Redis `GET` per message.
- Same fixed load run twice: `UseCache=true` (TTL-cached snapshot) then `UseCache=false` (GET/msg).
- Captures throughput, total Redis ops, avg Redis latency, avg processing latency.

Finding (local Redis): cached issued ~3 Redis ops for 50k messages vs. 50,000 uncached (~16,700×),
for a small local throughput delta that grows to 30–60% once Redis is across a network.

## Run

```bash
cd Processor && docker compose up -d zookeeper broker redis && cd ..
dotnet build Processor/Processor.csproj -c Release

loadtests/redis-cache-vs-lookup/seed_redis.sh       # 50 active settings
python3 loadtests/redis-cache-vs-lookup/gen_load.py # -> .work/load.txt
loadtests/redis-cache-vs-lookup/run.sh              # -> .work/result_cache_{on,off}.json
```

Compare `.work/result_cache_on.json` and `.work/result_cache_off.json` (throughput, `redis_ops`,
`avg_redis_latency_ms`). Env overrides: `N`, `WORK_DIR`, `PROJECT_ROOT`.

## Files

| File | Purpose |
|------|---------|
| `seed_redis.sh` | `SET datatypesettings:type-{0..49}` active |
| `gen_load.py` | keyed `load.txt` into `.work/` |
| `runload.py` | one mode: produce → drain → throughput/Redis-ops/latency → `result_<label>.json` |
| `parse_metrics.py` | standalone `/metrics` aggregate dump (debug helper) |
| `run.sh` | runs `cache_on` then `cache_off`, restarting the app between |
