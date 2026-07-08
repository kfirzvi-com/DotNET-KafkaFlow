#!/bin/bash
# Seed 50 INACTIVE data-type settings as per-key JSON strings.
# Each keyed message then triggers one Redis GET and filters out (no output produce).
set -eu
REDIS_CONTAINER="${REDIS_CONTAINER:-redis}"
PREFIX="${KEY_PREFIX:-datatypesettings:}"
COUNT="${TYPE_COUNT:-50}"

# Pass each JSON value as a single argv (redis-cli's inline parser would mangle piped quotes).
for i in $(seq 0 $((COUNT - 1))); do
  docker exec "$REDIS_CONTAINER" redis-cli SET "${PREFIX}type-$i" \
    "{\"dataTypeId\":\"type-$i\",\"isActive\":false}" >/dev/null
done

echo "seeded $COUNT inactive settings under ${PREFIX}type-*"
echo "sample: $(docker exec "$REDIS_CONTAINER" redis-cli GET "${PREFIX}type-0")"
