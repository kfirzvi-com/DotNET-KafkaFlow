#!/bin/bash
# Worker-count sweep for I/O-bound work (Redis GET per message, UseCache=false, FreeWorker).
set -u
ROOT="${PROJECT_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
HERE="$ROOT/loadtests/redis-io-workers"
WORK="${WORK_DIR:-$HERE/.work}"
PROJ="$ROOT/Processor/Processor.csproj"
export WORK_DIR="$WORK"
mkdir -p "$WORK"

N="${N:-200000}"
BUFFER="${BUFFER:-100}"
WORKERS=(${WORKERS_LIST:-4 16 32 64 128})

stop_app() {
  pkill -f "Processor.dll" 2>/dev/null
  pkill -f "dotnet run --project" 2>/dev/null
  for _ in $(seq 1 30); do
    curl -s -o /dev/null http://localhost:8080/metrics 2>/dev/null || return 0
    sleep 1
  done
}

start_app() {
  local w=$1 b=$2
  env DOTNET_ENVIRONMENT=Development \
      Logging__LogLevel__Default=Warning \
      Logging__LogLevel__KafkaFlow=Warning \
      "Logging__LogLevel__Confluent.Kafka=Warning" \
      Kafka__WorkersCount=$w \
      Kafka__BufferSize=$b \
      Benchmark__WorkMicros=0 \
      DataTypeSettings__UseCache=false \
      dotnet run --project "$PROJ" -c Release --no-build > "$WORK/app.log" 2>&1 &
  for _ in $(seq 1 90); do
    curl -s -o /dev/null http://localhost:8080/metrics 2>/dev/null && { sleep 3; return 0; }
    sleep 1
  done
  echo "APP FAILED TO START (w=$w b=$b)"; tail -5 "$WORK/app.log"; return 1
}

echo "io sweep start $(date) — workers: ${WORKERS[*]}, buffer=$BUFFER, N=$N"
for w in "${WORKERS[@]}"; do
  label="w${w}_b${BUFFER}"
  echo "--- run $label ---"
  stop_app
  start_app "$w" "$BUFFER" || { echo "skip $label"; continue; }
  python3 "$HERE/measure_io.py" "$label" "$N" "$w" "$BUFFER"
done
stop_app
echo "io sweep done $(date)"
