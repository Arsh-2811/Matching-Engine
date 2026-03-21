#!/usr/bin/env python3
"""
stress_test.py — Automated exchange stress test harness.

Launches exchange_app + load_gen, captures per-second MetricsThread output,
and writes two CSV files to tests/results/<timestamp>/:
  - per_second.csv  : one row per second (throughput, latency, fill rate, volume)
  - summary.csv     : one row with mean/p50/p95/p99 aggregates + all config params

Run multiple times with different --rate / --clients values (or edit config.hpp
and use --build --release) then pivot/compare in Excel.

Usage:
    python3 tests/stress_test.py [options]

Options:
    --duration  INT   Test duration in seconds        (default: 100)
    --clients   INT   Number of load-gen clients      (default: 4)
    --rate      INT   Orders/sec per client, 0=unlimited (default: 0)
    --build           Rebuild binaries before running
    --release         Use Release build               (requires --build)
    --output    DIR   Override results directory
"""

import argparse
import csv
import os
import queue
import re
import signal
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
CONFIG_HPP = ROOT / "include" / "exchange" / "common" / "config.hpp"
BUILD_DIR = ROOT / "build"

# ---------------------------------------------------------------------------
# Config extraction
# ---------------------------------------------------------------------------

def parse_config(path: Path) -> dict:
    """
    Extract constexpr values from config.hpp.
    Handles simple integer / arithmetic literals; skips anything unparseable.
    """
    text = path.read_text()
    pattern = re.compile(
        r'constexpr\s+[\w:<>]+\s+(\w+)\s*=\s*([0-9ULL\'*+\-/\s.e]+);'
    )
    config = {}
    for match in pattern.finditer(text):
        name = match.group(1)
        raw = match.group(2).strip()
        # Strip C++ integer suffixes and single-quote separators
        cleaned = re.sub(r"[ULul']", "", raw).strip()
        try:
            config[name] = eval(cleaned)  # arithmetic only, safe on config.hpp
        except Exception:
            config[name] = raw
    return config

# ---------------------------------------------------------------------------
# Metrics line parser
# ---------------------------------------------------------------------------

METRICS_RE = re.compile(
    r"Throughput:\s+([\d.]+)\s+orders/s"
    r".*?Avg Latency:\s+([\d.]+)\s+ns"
    r".*?P50 Latency:\s+([\d.]+)\s+ns"
    r".*?P99 Latency:\s+([\d.]+)\s+ns"
    r".*?P99\.9 Latency:\s+([\d.]+)\s+ns"
    r".*?Max Latency:\s+([\d.]+)\s+ns"
    r".*?Fill Rate:\s+([\d.]+)%"
    r".*?Volume:\s+([\d]+)"
)


def parse_metrics_line(line: str):
    m = METRICS_RE.search(line)
    if not m:
        return None
    return {
        "throughput_orders_per_s": float(m.group(1)),
        "avg_latency_ns":          float(m.group(2)),
        "p50_latency_ns":          float(m.group(3)),
        "p99_latency_ns":          float(m.group(4)),
        "p999_latency_ns":         float(m.group(5)),
        "max_latency_ns":          float(m.group(6)),
        "fill_rate_pct":           float(m.group(7)),
        "volume":                  int(m.group(8)),
    }

# ---------------------------------------------------------------------------
# Non-blocking line reader (background thread → queue)
# ---------------------------------------------------------------------------

def _reader_thread(stream, line_queue: queue.Queue):
    try:
        for line in stream:
            line_queue.put(line.rstrip())
    except Exception:
        pass
    finally:
        line_queue.put(None)  # sentinel

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build(release: bool):
    build_type = "Release" if release else "Debug"
    cmake_cfg = [
        "cmake", "-S", str(ROOT), "-B", str(BUILD_DIR),
        "-G", "Ninja", f"-DCMAKE_BUILD_TYPE={build_type}",
    ]
    print(f"[build] Configuring ({build_type})...")
    subprocess.run(cmake_cfg, check=True)
    print("[build] Compiling...")
    subprocess.run(["cmake", "--build", str(BUILD_DIR)], check=True)
    print("[build] Done.\n")

# ---------------------------------------------------------------------------
# Percentile helper
# ---------------------------------------------------------------------------

def percentile(data: list, p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    idx = max(0, min(int(len(s) * p / 100), len(s) - 1))
    return s[idx]

# ---------------------------------------------------------------------------
# Main test runner
# ---------------------------------------------------------------------------

def run_stress_test(duration: int, num_clients: int, rate: int,
                    results_dir: Path) -> Path:
    exchange_bin = BUILD_DIR / "exchange_app"
    loadgen_bin  = BUILD_DIR / "load_gen"

    for b in (exchange_bin, loadgen_bin):
        if not b.exists():
            sys.exit(f"[error] Binary not found: {b}. Run with --build first.")

    config = parse_config(CONFIG_HPP)

    # Create a timestamped sub-directory for this run
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = results_dir / ts
    run_dir.mkdir(parents=True, exist_ok=True)

    per_second_csv = run_dir / "per_second.csv"
    summary_csv    = run_dir / "summary.csv"

    # ---------- launch exchange ----------
    print("[run] Starting exchange_app...")
    exchange_proc = subprocess.Popen(
        [str(exchange_bin)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )

    # Wait until exchange signals it is live (or timeout)
    xq: queue.Queue = queue.Queue()
    threading.Thread(target=_reader_thread, args=(exchange_proc.stdout, xq),
                     daemon=True).start()

    live = False
    deadline_live = time.monotonic() + 20.0   # allow time for large heap allocs (~168 MB)
    while time.monotonic() < deadline_live:
        try:
            line = xq.get(timeout=0.2)
        except queue.Empty:
            continue
        if line is None:
            sys.exit("[error] exchange_app exited before becoming live.")
        print(f"  [exchange] {line}")
        if "LIVE" in line:
            live = True
            break

    if not live:
        exchange_proc.terminate()
        sys.exit("[error] Timed out waiting for exchange to start.")

    # ---------- launch load_gen ----------
    rate_str = "unlimited" if rate == 0 else f"{rate:,} orders/s/client"
    print(f"[run] Starting load_gen: {num_clients} clients @ {rate_str}")
    lg_args = [str(loadgen_bin), str(num_clients)]
    if rate > 0:
        lg_args.append(str(rate))
    loadgen_proc = subprocess.Popen(
        lg_args,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )
    lq: queue.Queue = queue.Queue()
    threading.Thread(target=_reader_thread, args=(loadgen_proc.stdout, lq),
                     daemon=True).start()

    # ---------- collect metrics ----------
    per_second_rows = []
    second = 0
    deadline_run = time.monotonic() + duration

    print(f"[run] Collecting for {duration}s  (Ctrl+C to abort early)\n")
    print(f"  {'Sec':>4}  {'Throughput':>12}  {'AvgLat':>10}  {'P50Lat':>10}  {'P99Lat':>10}  {'Fill%':>6}")
    print(f"  {'-'*4}  {'-'*12}  {'-'*10}  {'-'*10}  {'-'*10}  {'-'*6}")

    try:
        while time.monotonic() < deadline_run:
            try:
                line = xq.get(timeout=1.5)
            except queue.Empty:
                continue
            if line is None:
                print("[warn] exchange_app exited early.")
                break
            row = parse_metrics_line(line)
            if row:
                second += 1
                row["second"] = second
                per_second_rows.append(row)
                print(
                    f"  {second:>4}  "
                    f"{row['throughput_orders_per_s']/1e6:>10.2f}M/s  "
                    f"{row['avg_latency_ns']/1e6:>8.2f} ms  "
                    f"{row['p50_latency_ns']/1e6:>8.2f} ms  "
                    f"{row['p99_latency_ns']/1e6:>8.2f} ms  "
                    f"{row['fill_rate_pct']:>5.1f}%"
                )
    except KeyboardInterrupt:
        print("\n[run] Interrupted by user.")

    # ---------- stop processes ----------
    print("\n[run] Stopping load_gen...")
    try:
        loadgen_proc.send_signal(signal.SIGINT)
        loadgen_proc.wait(timeout=5)
    except Exception:
        loadgen_proc.kill()

    # Drain remaining exchange lines (up to 3 extra seconds)
    drain_deadline = time.monotonic() + 3.0
    while time.monotonic() < drain_deadline:
        try:
            line = xq.get(timeout=0.3)
        except queue.Empty:
            break
        if line is None:
            break
        row = parse_metrics_line(line)
        if row:
            second += 1
            row["second"] = second
            per_second_rows.append(row)

    print("[run] Stopping exchange_app...")
    try:
        exchange_proc.send_signal(signal.SIGTERM)
        exchange_proc.wait(timeout=5)
    except Exception:
        exchange_proc.kill()

    if not per_second_rows:
        print("[warn] No metrics rows collected — no CSV written.")
        return run_dir

    # ---------- compute summary ----------
    tputs     = [r["throughput_orders_per_s"] for r in per_second_rows]
    avg_lats  = [r["avg_latency_ns"]          for r in per_second_rows]
    p50_lats  = [r["p50_latency_ns"]          for r in per_second_rows]
    p99_lats  = [r["p99_latency_ns"]          for r in per_second_rows]
    p999_lats = [r["p999_latency_ns"]         for r in per_second_rows]
    max_lats  = [r["max_latency_ns"]          for r in per_second_rows]

    summary = {
        "seconds_collected":      len(per_second_rows),
        "total_orders":           sum(int(r["throughput_orders_per_s"]) for r in per_second_rows),
        # Throughput
        "mean_throughput":        statistics.mean(tputs),
        "p50_throughput":         percentile(tputs, 50),
        "p95_throughput":         percentile(tputs, 95),
        "p99_throughput":         percentile(tputs, 99),
        "min_throughput":         min(tputs),
        "max_throughput":         max(tputs),
        "stdev_throughput":       statistics.stdev(tputs) if len(tputs) > 1 else 0,
        # Average latency (mean across the per-second avg values)
        "mean_avg_latency_ns":    statistics.mean(avg_lats),
        "p50_avg_latency_ns":     percentile(avg_lats, 50),
        "p95_avg_latency_ns":     percentile(avg_lats, 95),
        "p99_avg_latency_ns":     percentile(avg_lats, 99),
        "min_avg_latency_ns":     min(avg_lats),
        "max_avg_latency_ns":     max(avg_lats),
        # Histogram-derived latency percentiles (aggregated across seconds)
        "mean_p50_latency_ns":    statistics.mean(p50_lats),
        "p99_p50_latency_ns":     percentile(p50_lats, 99),
        "mean_p99_latency_ns":    statistics.mean(p99_lats),
        "p99_p99_latency_ns":     percentile(p99_lats, 99),
        "mean_p999_latency_ns":   statistics.mean(p999_lats),
        "p99_p999_latency_ns":    percentile(p999_lats, 99),
        # Worst-case latency (max per second, includes OS scheduling spikes)
        "mean_max_latency_ns":    statistics.mean(max_lats),
        "p99_max_latency_ns":     percentile(max_lats, 99),
    }

    # Common metadata columns prefixed so they sort together in Excel
    run_meta = {
        "run_timestamp":        ts,
        "test_duration_s":      duration,
        "lg_num_clients":       num_clients,
        "lg_rate_per_client":   rate if rate > 0 else "unlimited",
        "lg_total_rate_target": (num_clients * rate) if rate > 0 else "unlimited",
    }
    cfg_meta = {f"cfg_{k}": v for k, v in config.items()}

    # ---------- write per_second.csv ----------
    ps_fields = (
        list(run_meta.keys()) + list(cfg_meta.keys()) +
        ["second", "throughput_orders_per_s", "avg_latency_ns",
         "p50_latency_ns", "p99_latency_ns", "p999_latency_ns",
         "max_latency_ns", "fill_rate_pct", "volume"]
    )
    with open(per_second_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ps_fields)
        w.writeheader()
        for row in per_second_rows:
            w.writerow({**run_meta, **cfg_meta, **row})

    # ---------- write summary.csv ----------
    sum_fields = list(run_meta.keys()) + list(cfg_meta.keys()) + list(summary.keys())
    summary_exists = summary_csv.exists()  # for append mode
    with open(summary_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=sum_fields)
        w.writeheader()
        w.writerow({**run_meta, **cfg_meta, **summary})

    # Also append this run to a global rolling summary (all runs in one file)
    global_csv = results_dir / "all_runs_summary.csv"
    global_exists = global_csv.exists()
    with open(global_csv, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=sum_fields)
        if not global_exists:
            w.writeheader()
        w.writerow({**run_meta, **cfg_meta, **summary})

    # ---------- print results ----------
    print(f"\n{'='*60}")
    print(f" STRESS TEST COMPLETE — {len(per_second_rows)} seconds collected")
    print(f"{'='*60}")
    print(f"  Mean throughput  : {summary['mean_throughput']/1e6:>8.2f} M orders/s")
    print(f"  P50  throughput  : {summary['p50_throughput']/1e6:>8.2f} M orders/s")
    print(f"  P95  throughput  : {summary['p95_throughput']/1e6:>8.2f} M orders/s")
    print(f"  Max  throughput  : {summary['max_throughput']/1e6:>8.2f} M orders/s")
    print(f"  Mean avg latency : {summary['mean_avg_latency_ns']/1e6:>8.2f} ms")
    print(f"  Mean P50 latency : {summary['mean_p50_latency_ns']/1e6:>8.2f} ms")
    print(f"  Mean P99 latency : {summary['mean_p99_latency_ns']/1e6:>8.2f} ms")
    print(f"  P99  P99 latency : {summary['p99_p99_latency_ns']/1e6:>8.2f} ms")
    print(f"  Mean P99.9 lat   : {summary['mean_p999_latency_ns']/1e6:>8.2f} ms")
    print(f"\n  per_second.csv  → {per_second_csv}")
    print(f"  summary.csv     → {summary_csv}")
    print(f"  all_runs_summary→ {global_csv}")

    return run_dir

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Exchange stress test harness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--duration", type=int, default=100,
                        help="Test duration in seconds (default: 100)")
    parser.add_argument("--clients",  type=int, default=4,
                        help="Number of load-gen clients (default: 4)")
    parser.add_argument("--rate",     type=int, default=0,
                        help="Orders/sec per client, 0=unlimited (default: 0)")
    parser.add_argument("--build",    action="store_true",
                        help="Rebuild binaries before running")
    parser.add_argument("--release",  action="store_true",
                        help="Use Release build (requires --build)")
    parser.add_argument("--output",   type=str, default=None,
                        help="Override results directory")
    args = parser.parse_args()

    if args.build:
        build(release=args.release)

    results_dir = (
        Path(args.output) if args.output
        else Path(__file__).parent / "results"
    )

    run_stress_test(
        duration=args.duration,
        num_clients=args.clients,
        rate=args.rate,
        results_dir=results_dir,
    )


if __name__ == "__main__":
    main()
