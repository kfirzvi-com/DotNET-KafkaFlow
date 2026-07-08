#!/usr/bin/env python3
"""Generate keyless load + warmup files for the CPU-bound worker/buffer sweep."""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.environ.get("WORK_DIR", os.path.join(HERE, ".work"))
os.makedirs(WORK, exist_ok=True)
N = int(os.environ.get("N", "300000"))

def gen(path, n):
    with open(path, "w") as f:
        for i in range(n):
            f.write('{"Id":"m%d","Content":"c"}\n' % i)

gen(os.path.join(WORK, "load_keyless.txt"), N)
gen(os.path.join(WORK, "warmup.txt"), 5000)
print(f"wrote load_keyless.txt ({N}) and warmup.txt (5000) in {WORK}")
