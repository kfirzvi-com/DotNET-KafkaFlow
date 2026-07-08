# Load tests

Reproducible load/performance experiments for the KafkaFlow processor. Each subdirectory is a
self-contained suite: generator + measurement + orchestrator + report generator, plus a README
with the exact steps.

| Experiment | Question | Report |
|------------|----------|--------|
| [`redis-cache-vs-lookup`](./redis-cache-vs-lookup) | Cost of caching data-type settings vs. a Redis lookup per message | `REDIS_CACHE_LOADTEST.pdf` |
| [`workers-buffer-tuning`](./workers-buffer-tuning) | Best `WorkersCount` × `BufferSize` for CPU-bound work (FreeWorker) | `WORKERS_BUFFER_TUNING.pdf` |
| [`redis-io-workers`](./redis-io-workers) | Effect of worker count on I/O-bound work (Redis `GET` per message) | `REDIS_IO_WORKERS.pdf` |

## Conventions

- **Prerequisites:** Docker stack up (`cd Processor && docker compose up -d zookeeper broker redis`),
  .NET 9 SDK, Python 3, and (for PDF rendering) Node + Playwright Chromium
  (`npx playwright install chromium`).
- **Paths:** scripts resolve the repo root via `PROJECT_ROOT` (default `git rev-parse --show-toplevel`)
  and write generated load files + intermediate results into a gitignored `.work/` under each suite.
- **App under test:** run natively with `DOTNET_ENVIRONMENT=Development` so it uses localhost brokers.
  Config knobs are passed as env vars (e.g. `Kafka__WorkersCount`, `Kafka__BufferSize`,
  `Benchmark__WorkMicros`, `DataTypeSettings__UseCache`).
- **Metrics:** scraped from the app's `/metrics` endpoint (`http://localhost:8080/metrics`).

Generated PDFs are kept out of git (see `.git/info/exclude`); regenerate them by running the suite.
