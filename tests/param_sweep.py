#!/usr/bin/env python3
"""
param_sweep.py — Automated parameter sweep for the exchange stress test.

Iterates over a matrix of (BATCH_SIZE, QUEUE_CAPACITY, rate_per_client),
patches config.hpp, rebuilds, and runs each configuration for --duration
seconds. All per-second rows and a consolidated summary land in
tests/results/sweep_<timestamp>/.

Usage:
    python3 tests/param_sweep.py [--duration 30]
"""

import argparse
import csv
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT       = Path(__file__).parent.parent
CONFIG_HPP = ROOT / "include" / "exchange" / "common" / "config.hpp"
STRESS_PY  = Path(__file__).parent / "stress_test.py"
BUILD_DIR  = ROOT / "build"

# ---------------------------------------------------------------------------
# Config patch helpers
# ---------------------------------------------------------------------------

def read_config() -> str:
    return CONFIG_HPP.read_text()

def patch_config(src: str, key: str, value: int) -> str:
    """Replace the first constexpr assignment for `key` with `value`."""
    pattern = re.compile(
        rf'(constexpr\s+[\w:<>]+\s+{re.escape(key)}\s*=\s*)([^;]+)(;)'
    )
    result, n = pattern.subn(rf'\g<1>{value}\3', src, count=1)
    if n == 0:
        raise RuntimeError(f"Key '{key}' not found in config.hpp")
    return result

def apply_config(patches: dict):
    src = read_config()
    for key, val in patches.items():
        src = patch_config(src, key, val)
    CONFIG_HPP.write_text(src)

def restore_config(original: str):
    CONFIG_HPP.write_text(original)

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def rebuild():
    print("  [build] Compiling...")
    r = subprocess.run(
        ["cmake", "--build", str(BUILD_DIR),
         "--target", "exchange_app", "--target", "load_gen"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        print(r.stderr[-2000:])
        sys.exit("[build] FAILED")
    print("  [build] OK")

# ---------------------------------------------------------------------------
# CSV merge helper
# ---------------------------------------------------------------------------

def append_to_master(master_csv: Path, run_dir: Path, label: str):
    ps_csv = run_dir / "per_second.csv"
    if not ps_csv.exists():
        return
    with open(ps_csv, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return
    for r in rows:
        r["sweep_label"] = label

    exists = master_csv.exists()
    fields = ["sweep_label"] + list(rows[0].keys())
    with open(master_csv, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            w.writeheader()
        w.writerows(rows)

def append_summary_to_master(master_sum: Path, run_dir: Path, label: str):
    s_csv = run_dir / "summary.csv"
    if not s_csv.exists():
        return
    with open(s_csv, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return
    for r in rows:
        r["sweep_label"] = label
    exists = master_sum.exists()
    fields = ["sweep_label"] + list(rows[0].keys())
    with open(master_sum, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            w.writeheader()
        w.writerows(rows)

# ---------------------------------------------------------------------------
# Sweep matrix
# ---------------------------------------------------------------------------

# Each entry: (label, config_patches, clients, rate_per_client)
# rate=0 means unlimited.
SWEEP = [
    # ── Baseline: BATCH=64, QUEUE=2M ──────────────────────────────────────
    ("B64_Q2M_unlimited",  {"BATCH_SIZE": 64, "QUEUE_CAPACITY": 2097152},  4, 0),
    ("B64_Q2M_r2000k",     {"BATCH_SIZE": 64, "QUEUE_CAPACITY": 2097152},  4, 2000000),
    ("B64_Q2M_r1400k",     {"BATCH_SIZE": 64, "QUEUE_CAPACITY": 2097152},  4, 1400000),
    ("B64_Q2M_r1000k",     {"BATCH_SIZE": 64, "QUEUE_CAPACITY": 2097152},  4, 1000000),
    # ── Bigger inbound queue: BATCH=64, QUEUE=4M ──────────────────────────
    ("B64_Q4M_unlimited",  {"BATCH_SIZE": 64, "QUEUE_CAPACITY": 4194304},  4, 0),
    ("B64_Q4M_r1400k",     {"BATCH_SIZE": 64, "QUEUE_CAPACITY": 4194304},  4, 1400000),
    # ── Small batch (lower latency): BATCH=8, QUEUE=2M ───────────────────
    ("B8_Q2M_unlimited",   {"BATCH_SIZE":  8, "QUEUE_CAPACITY": 2097152},  4, 0),
    ("B8_Q2M_r1400k",      {"BATCH_SIZE":  8, "QUEUE_CAPACITY": 2097152},  4, 1400000),
    ("B8_Q2M_r1000k",      {"BATCH_SIZE":  8, "QUEUE_CAPACITY": 2097152},  4, 1000000),
    # ── Mid batch: BATCH=16, QUEUE=2M ─────────────────────────────────────
    ("B16_Q2M_unlimited",  {"BATCH_SIZE": 16, "QUEUE_CAPACITY": 2097152},  4, 0),
    ("B16_Q2M_r1400k",     {"BATCH_SIZE": 16, "QUEUE_CAPACITY": 2097152},  4, 1400000),
    # ── BATCH=32 ──────────────────────────────────────────────────────────
    ("B32_Q2M_unlimited",  {"BATCH_SIZE": 32, "QUEUE_CAPACITY": 2097152},  4, 0),
    ("B32_Q2M_r1400k",     {"BATCH_SIZE": 32, "QUEUE_CAPACITY": 2097152},  4, 1400000),
]

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=30,
                    help="Seconds per run (default: 30)")
    args = ap.parse_args()

    original_config = read_config()

    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(__file__).parent / "results" / f"sweep_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    master_ps  = out_dir / "all_per_second.csv"
    master_sum = out_dir / "all_summary.csv"

    total = len(SWEEP)
    print(f"\n{'='*60}")
    print(f" PARAMETER SWEEP — {total} configurations × {args.duration}s each")
    print(f" Output: {out_dir}")
    print(f"{'='*60}\n")

    summary_rows = []

    try:
        last_patches = None
        for idx, (label, patches, clients, rate) in enumerate(SWEEP, 1):
            print(f"\n[{idx}/{total}] {label}")
            print(f"  Config: {patches}  clients={clients}  rate={rate or 'unlimited'}")

            # Only rebuild when config actually changes
            if patches != last_patches:
                apply_config(patches)
                rebuild()
                last_patches = patches

            run_subdir = out_dir / label
            result = subprocess.run(
                [
                    sys.executable, str(STRESS_PY),
                    "--duration", str(args.duration),
                    "--clients",  str(clients),
                    "--rate",     str(rate),
                    "--output",   str(run_subdir.parent),
                ],
                capture_output=False,   # let output stream live
                text=True,
            )

            # stress_test.py writes into results/<timestamp>/ — find it
            # (it creates its own timestamped subdir inside out_dir/<label>)
            # We pass out_dir/<label> as --output so it lands there directly.
            # The script creates a <TS>/ inside that.  Grab the latest.
            run_subdir.parent.mkdir(parents=True, exist_ok=True)
            subdirs = sorted(run_subdir.parent.iterdir()) if run_subdir.parent.exists() else []
            if subdirs:
                actual_run = subdirs[-1]
                append_to_master(master_ps, actual_run, label)
                append_summary_to_master(master_sum, actual_run, label)

                # Collect summary row for final table
                s_csv = actual_run / "summary.csv"
                if s_csv.exists():
                    with open(s_csv, newline="") as f:
                        rows = list(csv.DictReader(f))
                    if rows:
                        summary_rows.append((label, rows[0]))

    finally:
        print("\n[sweep] Restoring original config.hpp...")
        restore_config(original_config)
        rebuild()   # restore binaries to original config

    # ---- Print consolidated results table ----
    print(f"\n\n{'='*90}")
    print(f" SWEEP RESULTS SUMMARY")
    print(f"{'='*90}")
    print(
        f"  {'Label':<22} {'MeanTput':>10} {'P50Tput':>10} "
        f"{'MeanAvgLat':>12} {'P99AvgLat':>12} {'P99MaxLat':>12}"
    )
    print(f"  {'-'*22} {'-'*10} {'-'*10} {'-'*12} {'-'*12} {'-'*12}")
    for label, row in summary_rows:
        print(
            f"  {label:<22} "
            f"{float(row.get('mean_throughput', 0))/1e6:>9.2f}M "
            f"{float(row.get('p50_throughput',  0))/1e6:>9.2f}M "
            f"{float(row.get('mean_avg_latency_ns', 0))/1e6:>11.2f}ms "
            f"{float(row.get('p99_avg_latency_ns',  0))/1e6:>11.2f}ms "
            f"{float(row.get('p99_max_latency_ns',  0))/1e6:>11.2f}ms"
        )

    print(f"\n  Master per-second CSV : {master_ps}")
    print(f"  Master summary CSV    : {master_sum}")
    print(f"{'='*90}\n")


if __name__ == "__main__":
    main()
