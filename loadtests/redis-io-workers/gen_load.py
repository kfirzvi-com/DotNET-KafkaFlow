#!/usr/bin/env python3
"""Generate keyed load + warmup files (key = data-type id -> triggers a Redis GET)."""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.environ.get("WORK_DIR", os.path.join(HERE, ".work"))
os.makedirs(WORK, exist_ok=True)

N = int(os.environ.get("N", "200000"))
TYPES = int(os.environ.get("TYPE_COUNT", "50"))

def gen(path, n):
    with open(path, "w") as f:
        for i in range(n):
            f.write('type-%d|{"Id":"m%d","Content":"c"}\n' % (i % TYPES, i))

gen(os.path.join(WORK, "io_load.txt"), N)
gen(os.path.join(WORK, "io_warmup.txt"), 5000)
print(f"wrote io_load.txt ({N}) and io_warmup.txt (5000), keyed type-0..{TYPES-1} in {WORK}")
