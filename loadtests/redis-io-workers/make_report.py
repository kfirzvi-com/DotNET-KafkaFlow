#!/usr/bin/env python3
"""Build report.html from io_w*.json results (I/O-bound worker sweep)."""
import glob, json, os

HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.environ.get("WORK_DIR", os.path.join(HERE, ".work"))
CORES = int(os.environ.get("HOST_CORES", "18"))

def load():
    rows = []
    for p in glob.glob(os.path.join(WORK, "io_w*.json")):
        rows.append(json.load(open(p)))
    return sorted(rows, key=lambda r: r["workers"])

def main():
    rows = load()
    if not rows:
        print("no results in", WORK); return
    hi = max(r["throughput_msg_s"] for r in rows)
    peak = max(rows, key=lambda r: r["throughput_msg_s"])
    lo_w = rows[0]
    # scaling factors between consecutive worker counts
    def bars():
        out = ""
        for r in rows:
            pct = 100 * r["throughput_msg_s"] / hi if hi else 0
            out += (f'<div class="bar-row"><div class="bar-label">{r["workers"]} workers</div>'
                    f'<div class="bar-track"><div class="bar-fill" style="width:{pct:.1f}%">{r["throughput_msg_s"]:,.0f}</div></div></div>')
        return out
    trows = ""
    for r in rows:
        star = " ★" if r is peak else ""
        trows += (f"<tr><td class='num'>{r['workers']}</td>"
                  f"<td class='num'><b>{r['throughput_msg_s']:,.0f}</b>{star}</td>"
                  f"<td class='num'>{r['cpu_cores']}</td>"
                  f"<td class='num'>{r.get('redis_ops_per_msg')}</td>"
                  f"<td class='num'>{r.get('avg_redis_ms')}</td>"
                  f"<td class='num'>{r['avg_proc_ms']}</td>"
                  f"<td class='num'>{r['mem_peak_mb']:,.0f}</td></tr>")
    peak_cpu_util = peak["cpu_cores"] / CORES * 100
    html = f"""<!doctype html><html><head><meta charset="utf-8"><style>
    @page {{ size: A4; margin: 16mm 15mm; }} * {{ box-sizing:border-box; }}
    html {{ -webkit-print-color-adjust:exact; print-color-adjust:exact; }}
    body {{ font-family:-apple-system,"Segoe UI",Helvetica,Arial,sans-serif; color:#1c2530; font-size:10.5px; line-height:1.5; margin:0; }}
    h1 {{ font-size:22px; margin:0 0 2px; color:#0b2e4f; }}
    h2 {{ font-size:14px; margin:20px 0 6px; color:#0b2e4f; border-bottom:2px solid #2f7ec4; padding-bottom:3px; }}
    .sub {{ color:#5b6b7a; font-size:10px; }} .meta {{ margin:8px 0 0; font-size:9.5px; color:#5b6b7a; }}
    code {{ font-family:"SF Mono",Menlo,Consolas,monospace; font-size:9px; color:#0b2e4f; }}
    table {{ width:100%; border-collapse:collapse; margin:6px 0; }}
    th,td {{ text-align:left; padding:4px 8px; border:1px solid #d8e0e8; }}
    th {{ background:#eef3f8; color:#0b2e4f; font-weight:600; }} td.num,th.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
    tr:nth-child(even) td {{ background:#f7fafc; }}
    .tldr {{ background:#0e2436; color:#e8eef4; border-radius:6px; padding:10px 14px; margin:10px 0; }}
    .tldr b {{ color:#8fd3ff; }}
    .key {{ background:#eef5fc; border-left:3px solid #2f7ec4; padding:8px 12px; margin:8px 0; border-radius:0 4px 4px 0; }}
    .key b {{ color:#1f5c96; }}
    .chart-title {{ font-size:10px; font-weight:600; color:#12405f; margin:8px 0 4px; }}
    .bar-row {{ display:flex; align-items:center; margin:3px 0; gap:8px; }}
    .bar-label {{ width:80px; font-size:9px; text-align:right; color:#33475b; }}
    .bar-track {{ flex:1; background:#eef3f8; border-radius:3px; height:18px; }}
    .bar-fill {{ height:100%; border-radius:3px; background:#2f7ec4; color:#fff; font-size:9px; font-weight:600; display:flex; align-items:center; justify-content:flex-end; padding-right:6px; white-space:nowrap; }}
    .note {{ background:#fff6e9; border:1px solid #f0d29a; border-radius:5px; padding:7px 10px; margin:6px 0; font-size:9.8px; }}
    ul {{ margin:4px 0 8px; padding-left:18px; }} li {{ margin:2px 0; }}
    footer {{ margin-top:14px; border-top:1px solid #d8e0e8; padding-top:6px; color:#8593a1; font-size:8.5px; }}
    </style></head><body>
    <h1>Worker Count vs. I/O-Bound Work — Load Test Report</h1>
    <div class="sub">One Redis <code>GET</code> per message (data-type settings, <code>UseCache=false</code>), FreeWorker strategy, keyless.</div>
    <div class="meta">Host: {CORES} logical CPUs · buffer {rows[0]['buffer']} · {rows[0]['messages']:,} msg/run · backlog-drain · one Redis GET/msg then filtered (no output produce)</div>

    <div class="tldr"><b>TL;DR.</b> For I/O-bound work, adding workers <b>past the core count keeps helping</b>:
    peak <b>{peak['throughput_msg_s']:,.0f} msg/s at {peak['workers']} workers</b> while using only {peak['cpu_cores']} CPU cores
    ({peak_cpu_util:.0f}% of {CORES}) — the threads spend most of their time awaiting Redis, not burning CPU.
    This is the opposite of the CPU-bound sweep, where throughput flat-lined at the core count. Each message costs
    one Redis GET (~{peak.get('avg_redis_ms')} ms); throughput scales with concurrency until Redis / the connection multiplexer saturates.</div>

    <h2>1. Throughput vs workers</h2>
    {bars()}
    <div class="key"><b>Workers &gt; cores is correct for I/O-bound work.</b> Because each worker <code>await</code>s the
    Redis round-trip, the OS thread is free during the wait, so more workers overlap more in-flight Redis requests.
    Throughput rises well beyond the {CORES}-core count; CPU stays low ({lo_w['cpu_cores']}→{peak['cpu_cores']} cores) because the bottleneck is Redis latency, not CPU.</div>

    <h2>2. Full results</h2>
    <table>
    <tr><th class="num">workers</th><th class="num">throughput msg/s</th><th class="num">CPU cores</th><th class="num">Redis ops/msg</th><th class="num">avg Redis ms</th><th class="num">avg proc ms</th><th class="num">peak MB</th></tr>
    {trows}
    </table>
    <div class="note"><b>Redis is the shared bottleneck.</b> Every message is a GET, so Redis op rate ≈ throughput and
    scales with the whole fleet. Past the point where Redis latency climbs under load, more workers stop helping and
    just queue on the multiplexer. Tune workers up until throughput plateaus or Redis latency (p99) degrades.</div>

    <h2>3. Recommendation</h2>
    <ul>
    <li><b>I/O-bound ⇒ workers ≫ cores.</b> Size workers to keep enough Redis requests in flight to hide latency, not to the CPU. Here the knee is around {peak['workers']} workers.</li>
    <li><b>But prefer caching.</b> This whole workload exists only because <code>UseCache=false</code>; with the cache on, the per-message GET disappears (see the <code>redis-cache-vs-lookup</code> report) and the consumer becomes CPU-bound again (tune per <code>workers-buffer-tuning</code>).</li>
    <li><b>Watch Redis latency + op rate</b> (<code>redis_operation_duration_milliseconds</code>, <code>redis_operations_total</code>): they are the real ceiling for the uncached path, and they scale with pod count.</li>
    </ul>
    <div class="note"><b>Caveats:</b> (1) at the highest worker counts throughput is likely bounded by the single-process
    test producer (~80–85k msg/s), so the consumer's true ceiling is <i>at least</i> the figures shown — note CPU never
    saturates ({peak['cpu_cores']} of {CORES} cores) and Redis latency keeps rising, both signs the consumer had headroom.
    (2) Single local Redis + one connection multiplexer + 18-core host shared with Docker. Absolute numbers are local;
    the shape — throughput scales with workers far beyond the core count, at low CPU, until Redis saturates — is what transfers.</div>
    <footer>loadtests/redis-io-workers · UseCache=false · FreeWorker · {len(rows)} configs</footer>
    </body></html>"""
    with open(os.path.join(WORK, "report.html"), "w") as f:
        f.write(html)
    print(f"wrote report.html ({len(rows)} configs). peak={peak['workers']}w @ {peak['throughput_msg_s']:,.0f} msg/s, {peak['cpu_cores']} cores")

if __name__ == "__main__":
    main()
