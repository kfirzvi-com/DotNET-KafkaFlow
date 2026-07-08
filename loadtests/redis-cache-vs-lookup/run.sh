#!/bin/bash
# Cache vs. per-lookup: run the same load with DataTypeSettings:UseCache true then false.
set -u
ROOT="${PROJECT_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
HERE="$(cd "$(dirname "$0")" && pwd)"
SP="${WORK_DIR:-$HERE/.work}"
PROJ="$ROOT/Processor/Processor.csproj"
export WORK_DIR="$SP"
mkdir -p "$SP"
N="${N:-50000}"

stop_app() {
  pkill -f "Processor.dll" 2>/dev/null; pkill -f "dotnet run --project" 2>/dev/null
  for _ in $(seq 1 30); do curl -s -o /dev/null http://localhost:8080/metrics 2>/dev/null || return 0; sleep 1; done
}
run_mode() {
  local label=$1 usecache=$2
  stop_app
  env DOTNET_ENVIRONMENT=Development \
      Logging__LogLevel__Default=Warning Logging__LogLevel__KafkaFlow=Warning "Logging__LogLevel__Confluent.Kafka=Warning" \
      Kafka__WorkersCount=10 Kafka__BufferSize=100 \
      DataTypeSettings__UseCache=$usecache \
      dotnet run --project "$PROJ" -c Release --no-build > "$SP/app.log" 2>&1 &
  for _ in $(seq 1 90); do curl -s -o /dev/null http://localhost:8080/metrics 2>/dev/null && { sleep 3; break; }; sleep 1; done
  python3 "$HERE/runload.py" "$label" "$N"
}

run_mode cache_on true
run_mode cache_off false
stop_app
echo "done -> $SP/result_cache_on.json, $SP/result_cache_off.json"
