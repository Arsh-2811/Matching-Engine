#!/usr/bin/env python3
"""
rate_sweep_plot.py — Sweep rate limits, plot throughput vs latency curve.

Runs stress tests at increasing rate limits with BATCH=64, 4 clients,
then plots a dual-axis chart: throughput (left) and P50 latency (right)
against total input rate. Saves the chart and prints the sweet spot.

Usage:
    python3 tests/rate_sweep_plot.py [--duration INT]
"""

import argparse
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
BUILD_DIR = ROOT / "build"
RESULTS_DIR = ROOT / "tests" / "results"

METRICS_RE = re.compile(
    r"Throughput:\s+([\d.]+)\s+orders/s"
    r".*?Avg Latency:\s+([\d.]+)\s+ns"
    r".*?P50 Latency:\s+([\d.]+)\s+ns"
    r".*?P99 Latency:\s+([\d.]+)\s+ns"
)

NUM_CLIENTS = 4

# Rate limits per client to sweep (0 = unlimited)
RATES_PER_CLIENT = [200_000, 400_000, 700_000, 1_000_000, 1_200_000,
                    1_400_000, 1_700_000, 2_000_000, 2_500_000, 3_000_000, 0]


def _reader_thread(stream, q: queue.Queue):
    try:
        for line in stream:
            q.put(line.rstrip())
    except Exception:
        pass
    finally:
        q.put(None)


def run_one(rate_per_client: int, duration: int) -> dict | None:
    label = "unlimited" if rate_per_client == 0 else f"{rate_per_client // 1000}k/client"
    print(f"\n[sweep] rate={label}  duration={duration}s")

    exchange_bin = BUILD_DIR / "exchange_app"
    loadgen_bin  = BUILD_DIR / "load_gen"

    exchange_proc = subprocess.Popen(
        [str(exchange_bin)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )
    xq: queue.Queue = queue.Queue()
    threading.Thread(target=_reader_thread, args=(exchange_proc.stdout, xq),
                     daemon=True).start()

    # Wait for exchange to be live
    live = False
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        try:
            line = xq.get(timeout=0.5)
        except queue.Empty:
            continue
        if line is None:
            break
        if "LIVE" in line:
            live = True
            break

    if not live:
        exchange_proc.kill()
        exchange_proc.wait()
        print("[sweep] exchange failed to start, skipping")
        return None

    time.sleep(0.2)

    # Always pass rate explicitly — 0 means unlimited in load_gen argv[2].
    # Omitting argv[2] uses DEFAULT_RATE_LIMIT from config.hpp (not necessarily 0).
    lg_args = [str(loadgen_bin), str(NUM_CLIENTS), str(rate_per_client)]
    lg_proc = subprocess.Popen(
        lg_args,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )
    lq: queue.Queue = queue.Queue()
    threading.Thread(target=_reader_thread, args=(lg_proc.stdout, lq),
                     daemon=True).start()

    # Collect metrics for `duration` seconds (skip first 2s warmup)
    throughputs = []
    p50_latencies = []
    p99_latencies = []

    start = time.monotonic()
    warmup = 2.0

    while time.monotonic() - start < duration + warmup:
        try:
            line = xq.get(timeout=0.3)
        except queue.Empty:
            continue
        if line is None:
            break
        m = METRICS_RE.search(line)
        if m and (time.monotonic() - start) > warmup:
            tput = float(m.group(1))
            p50  = float(m.group(3))
            p99  = float(m.group(4))
            throughputs.append(tput)
            p50_latencies.append(p50)
            p99_latencies.append(p99)
            elapsed = time.monotonic() - start - warmup
            print(f"  t={elapsed:4.0f}s  tput={tput/1e6:6.2f}M/s  "
                  f"P50={p50/1e6:7.3f}ms  P99={p99/1e6:7.2f}ms")

    # Stop load gen then exchange
    lg_proc.terminate()
    try:
        lg_proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        lg_proc.kill()
        lg_proc.wait()

    exchange_proc.terminate()
    try:
        exchange_proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        exchange_proc.kill()
        exchange_proc.wait()

    # Drain queues
    for q_ in (xq, lq):
        while True:
            try:
                q_.get_nowait()
            except queue.Empty:
                break

    if not throughputs:
        print("[sweep] no metrics collected, skipping")
        return None

    n = len(throughputs)
    mean_tput = sum(throughputs) / n
    median_tput = sorted(throughputs)[n // 2]
    mean_p50 = sum(p50_latencies) / n
    mean_p99 = sum(p99_latencies) / n

    print(f"  → mean tput={mean_tput/1e6:.2f}M/s  "
          f"median tput={median_tput/1e6:.2f}M/s  "
          f"mean P50={mean_p50/1e6:.3f}ms  mean P99={mean_p99/1e6:.2f}ms")

    return {
        "rate_per_client": rate_per_client,
        "total_rate": rate_per_client * NUM_CLIENTS if rate_per_client > 0 else 0,
        "mean_tput": mean_tput,
        "median_tput": median_tput,
        "mean_p50_ms": mean_p50 / 1e6,
        "mean_p99_ms": mean_p99 / 1e6,
    }


def plot_results(results: list[dict], out_path: Path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    import numpy as np

    # Sort by total_rate (put unlimited at the end)
    rated = [r for r in results if r["total_rate"] > 0]
    unlimited = [r for r in results if r["total_rate"] == 0]
    rated.sort(key=lambda r: r["total_rate"])

    # X labels
    labels = []
    tputs  = []
    p50s   = []
    p99s   = []

    for r in rated:
        labels.append(f"{r['rate_per_client']//1000}k")
        tputs.append(r["mean_tput"] / 1e6)
        p50s.append(r["mean_p50_ms"])
        p99s.append(r["mean_p99_ms"])

    for r in unlimited:
        labels.append("unlimited")
        tputs.append(r["mean_tput"] / 1e6)
        p50s.append(r["mean_p50_ms"])
        p99s.append(r["mean_p99_ms"])

    x = np.arange(len(labels))

    fig, ax1 = plt.subplots(figsize=(13, 6))
    fig.patch.set_facecolor("#0d1117")
    ax1.set_facecolor("#161b22")

    # Throughput bars
    bars = ax1.bar(x, tputs, color="#238636", alpha=0.75, width=0.4, label="Mean Throughput")
    ax1.set_ylabel("Throughput (M orders/s)", color="#c9d1d9", fontsize=11)
    ax1.tick_params(axis="y", colors="#c9d1d9")
    ax1.tick_params(axis="x", colors="#c9d1d9")
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, rotation=30, ha="right", color="#c9d1d9")
    ax1.set_xlabel("Rate limit per client (4 clients total, BATCH=64)", color="#c9d1d9", fontsize=11)
    ax1.yaxis.label.set_color("#c9d1d9")
    ax1.spines[:].set_color("#30363d")
    ax1.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1f M"))

    # Latency lines on second axis
    ax2 = ax1.twinx()
    ax2.set_facecolor("#161b22")
    ax2.plot(x, p50s, color="#f78166", linewidth=2, marker="o", markersize=6, label="P50 Latency")
    ax2.plot(x, p99s, color="#ffa657", linewidth=1.5, marker="s", markersize=5,
             linestyle="--", alpha=0.8, label="P99 Latency")
    ax2.set_ylabel("Latency (ms)", color="#c9d1d9", fontsize=11)
    ax2.tick_params(axis="y", colors="#c9d1d9")
    ax2.yaxis.label.set_color("#c9d1d9")
    ax2.spines[:].set_color("#30363d")
    ax2.set_yscale("log")
    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda v, _: f"{v:.2f}ms" if v < 1 else f"{v:.1f}ms"
    ))

    # Find sweet spot: last rate where P50 < 0.5ms
    sweet_idx = None
    for i, (p50, rate) in enumerate(zip(p50s, labels)):
        if p50 < 0.5 and rate != "unlimited":
            sweet_idx = i

    if sweet_idx is not None:
        ax1.axvline(x=sweet_idx, color="#58a6ff", linewidth=1.5,
                    linestyle=":", alpha=0.9, label="Sweet spot")
        ax1.annotate(
            f"Sweet spot\n{labels[sweet_idx]}/client\n"
            f"{tputs[sweet_idx]:.1f}M/s @ {p50s[sweet_idx]:.3f}ms",
            xy=(sweet_idx, tputs[sweet_idx]),
            xytext=(sweet_idx + 0.5, tputs[sweet_idx] * 0.7),
            color="#58a6ff", fontsize=9,
            arrowprops=dict(arrowstyle="->", color="#58a6ff"),
        )

    # Legend
    lines1, labs1 = ax1.get_legend_handles_labels()
    lines2, labs2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labs1 + labs2,
               loc="upper left", facecolor="#161b22",
               edgecolor="#30363d", labelcolor="#c9d1d9", fontsize=9)

    plt.title("Throughput vs Latency — Rate Sweep (master, BATCH=64, 4 clients)",
              color="#c9d1d9", fontsize=13, pad=12)
    plt.tight_layout()
    plt.savefig(str(out_path), dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close()
    print(f"\n[plot] saved → {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=15,
                        help="Seconds per rate point (default 15)")
    args = parser.parse_args()

    for b in ("exchange_app", "load_gen"):
        if not (BUILD_DIR / b).exists():
            sys.exit(f"[error] {b} not found in build/. Run cmake --build build first.")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    for rate in RATES_PER_CLIENT:
        result = run_one(rate, args.duration)
        if result:
            results.append(result)
        time.sleep(1.0)  # let ports clear between runs

    if not results:
        sys.exit("[error] no results collected")

    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = RESULTS_DIR / f"rate_sweep_{ts}.png"
    plot_results(results, out_path)

    # Print summary table
    print("\n" + "=" * 65)
    print(f"{'Rate/client':<14} {'Total rate':>12} {'Mean tput':>12} {'P50 lat':>10} {'P99 lat':>10}")
    print("-" * 65)
    for r in results:
        rate_str = "unlimited" if r["rate_per_client"] == 0 else f"{r['rate_per_client']//1000}k"
        total_str = "—" if r["total_rate"] == 0 else f"{r['total_rate']//1000}k"
        print(f"{rate_str:<14} {total_str:>12} {r['mean_tput']/1e6:>10.2f}M/s "
              f"{r['mean_p50_ms']:>9.3f}ms {r['mean_p99_ms']:>9.2f}ms")

    print(f"\n[done] chart saved to {out_path}")
    return str(out_path)


if __name__ == "__main__":
    main()
