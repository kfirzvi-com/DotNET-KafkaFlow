#!/usr/bin/env python3
"""Generate a keyed load file (key = active data-type id) for the cache-vs-lookup test."""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.environ.get("WORK_DIR", os.path.join(HERE, ".work"))
os.makedirs(WORK, exist_ok=True)
N = int(os.environ.get("N", "50000"))
TYPES = int(os.environ.get("TYPE_COUNT", "50"))

with open(os.path.join(WORK, "load.txt"), "w") as f:
    for i in range(N):
        f.write('type-%d|{"Id":"m%d","Content":"c"}\n' % (i % TYPES, i))
print(f"wrote load.txt ({N}) keyed type-0..{TYPES-1} in {WORK}")
