#!/bin/bash
# Seed 50 ACTIVE data-type settings (per-key JSON strings) so keyed messages are processed,
# exercising the per-message Redis lookup in the uncached mode.
set -eu
REDIS_CONTAINER="${REDIS_CONTAINER:-redis}"
PREFIX="${KEY_PREFIX:-datatypesettings:}"
COUNT="${TYPE_COUNT:-50}"

for i in $(seq 0 $((COUNT - 1))); do
  docker exec "$REDIS_CONTAINER" redis-cli SET "${PREFIX}type-$i" \
    "{\"dataTypeId\":\"type-$i\",\"isActive\":true}" >/dev/null
done
echo "seeded $COUNT active settings under ${PREFIX}type-*"
