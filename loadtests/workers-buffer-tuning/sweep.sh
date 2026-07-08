#!/bin/bash
# Workers x Buffer sweep under FreeWorker strategy, keyless workload.
set -u
ROOT="${PROJECT_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
SP="${WORK_DIR:-$(dirname "$0")/.work}"
PROJ="$ROOT/Processor/Processor.csproj"
export WORK_DIR="$SP"
mkdir -p "$SP"
N=300000
WORK=700
WORKERS=(4 16 32)
BUFFERS=(10 100 1000)

stop_app() {
  pkill -f "Processor.dll" 2>/dev/null
  pkill -f "dotnet run --project" 2>/dev/null
  # wait until metrics endpoint is down (port freed)
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
      Benchmark__WorkMicros=$WORK \
      dotnet run --project "$PROJ" -c Release --no-build > "$SP/app_sweep.log" 2>&1 &
  # wait ready
  for _ in $(seq 1 90); do
    curl -s -o /dev/null http://localhost:8080/metrics 2>/dev/null && return 0
    sleep 1
  done
  echo "APP FAILED TO START (w=$w b=$b)"; tail -5 "$SP/app_sweep.log"; return 1
}

echo "sweep start $(date)"
for w in "${WORKERS[@]}"; do
  for b in "${BUFFERS[@]}"; do
    label="w${w}_b${b}"
    echo "--- run $label ---"
    stop_app
    start_app "$w" "$b" || { echo "skip $label"; continue; }
    python3 "$(dirname "$0")/measure_run.py" "$label" "$N" "$w" "$b" "$WORK"
  done
done
stop_app
echo "sweep done $(date)"
