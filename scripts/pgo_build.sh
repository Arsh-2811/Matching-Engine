#!/usr/bin/env bash
# scripts/pgo_build.sh
#
# Full PGO workflow: PGOGen build → 30s profile run → PGOUse build
#
# Usage:
#   bash scripts/pgo_build.sh
#
# Requirements:
#   - GCC 10+ (tested on GCC 13 / Ubuntu 24.04)
#   - Port 9000 must be free before running
#
# Output:
#   build_pgo/exchange_app  — PGO-optimised exchange server
#   build_pgo/load_gen      — PGO-optimised load generator
#   build_pgo/pgo-profiles/ — raw .gcda profile data (keep for re-use)

set -euo pipefail

# ---------------------------------------------------------------------------
# 0. Resolve paths
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"
REPO_ROOT="$(realpath "${SCRIPT_DIR}/..")"
BUILD_DIR="${REPO_ROOT}/build_pgo"
PROFILE_DIR="${BUILD_DIR}/pgo-profiles"

EXCHANGE_BIN="${BUILD_DIR}/exchange_app"
LOADGEN_BIN="${BUILD_DIR}/load_gen"

LIVE_TIMEOUT_S=20       # seconds to wait for exchange to print LIVE
SHUTDOWN_TIMEOUT_S=15   # seconds to wait for clean shutdown message
PROFILE_DURATION_S=30   # seconds to run load_gen for profiling
CLIENTS=4
RATE_PER_CLIENT=1400000

echo "=== PGO Build Script ==="
echo "  Repo root  : ${REPO_ROOT}"
echo "  Build dir  : ${BUILD_DIR}"
echo "  Profile dir: ${PROFILE_DIR}"
echo ""

# ---------------------------------------------------------------------------
# 1. Configure and build PGOGen binaries
# ---------------------------------------------------------------------------

echo "[1/3] Configuring PGOGen build..."
cmake -S "${REPO_ROOT}" -B "${BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE=PGOGen

echo "[1/3] Building instrumented exchange_app and load_gen..."
cmake --build "${BUILD_DIR}" --target exchange_app load_gen -- -j"$(nproc)"
echo "[1/3] PGOGen build complete."
echo ""

# ---------------------------------------------------------------------------
# 2. Profile run
# ---------------------------------------------------------------------------

echo "[2/3] Creating profile directory..."
mkdir -p "${PROFILE_DIR}"

echo "[2/3] Starting instrumented exchange_app..."
"${EXCHANGE_BIN}" > "${BUILD_DIR}/exchange_stdout.log" 2>&1 &
EXCHANGE_PID=$!
echo "  exchange_app PID: ${EXCHANGE_PID}"

# Poll log for "LIVE" with timeout
LIVE_DEADLINE=$(( $(date +%s) + LIVE_TIMEOUT_S ))
LIVE_SEEN=0
while [ "$(date +%s)" -lt "${LIVE_DEADLINE}" ]; do
    if grep -q "LIVE" "${BUILD_DIR}/exchange_stdout.log" 2>/dev/null; then
        LIVE_SEEN=1
        break
    fi
    sleep 0.2
done

if [ "${LIVE_SEEN}" -eq 0 ]; then
    echo "[error] Timed out waiting for exchange_app to print LIVE." >&2
    cat "${BUILD_DIR}/exchange_stdout.log" >&2
    kill "${EXCHANGE_PID}" 2>/dev/null || true
    wait "${EXCHANGE_PID}" 2>/dev/null || true
    exit 1
fi
echo "  exchange_app is LIVE."

echo "[2/3] Starting load_gen: ${CLIENTS} clients @ ${RATE_PER_CLIENT} orders/s each..."
"${LOADGEN_BIN}" "${CLIENTS}" "${RATE_PER_CLIENT}" > "${BUILD_DIR}/loadgen_stdout.log" 2>&1 &
LOADGEN_PID=$!
echo "  load_gen PID: ${LOADGEN_PID}"

echo "  Profiling for ${PROFILE_DURATION_S} seconds..."
sleep "${PROFILE_DURATION_S}"

# Stop load_gen cleanly
echo "[2/3] Sending SIGINT to load_gen (PID ${LOADGEN_PID})..."
kill -SIGINT "${LOADGEN_PID}" 2>/dev/null || true
wait "${LOADGEN_PID}" 2>/dev/null || true
echo "  load_gen has exited."

# Stop exchange cleanly — gcov runtime flushes .gcda files on exit
echo "[2/3] Sending SIGINT to exchange_app (PID ${EXCHANGE_PID})..."
kill -SIGINT "${EXCHANGE_PID}" 2>/dev/null || true

SHUTDOWN_DEADLINE=$(( $(date +%s) + SHUTDOWN_TIMEOUT_S ))
CLEAN_SHUTDOWN=0
while [ "$(date +%s)" -lt "${SHUTDOWN_DEADLINE}" ]; do
    if grep -q "Exchange shut down cleanly." "${BUILD_DIR}/exchange_stdout.log" 2>/dev/null; then
        CLEAN_SHUTDOWN=1
        break
    fi
    sleep 0.2
done

if [ "${CLEAN_SHUTDOWN}" -eq 0 ]; then
    echo "[warn] Timed out waiting for clean shutdown — sending SIGKILL." >&2
    kill -SIGKILL "${EXCHANGE_PID}" 2>/dev/null || true
fi
wait "${EXCHANGE_PID}" 2>/dev/null || true
echo "  exchange_app has exited."

# Verify .gcda files were produced
GCDA_COUNT=$(find "${PROFILE_DIR}" -name "*.gcda" 2>/dev/null | wc -l)
if [ "${GCDA_COUNT}" -eq 0 ]; then
    echo "[error] No .gcda files found in ${PROFILE_DIR}." >&2
    echo "  The exchange may not have exited cleanly enough for gcov to flush." >&2
    echo "  Exchange log:" >&2
    cat "${BUILD_DIR}/exchange_stdout.log" >&2
    exit 1
fi
echo "  Found ${GCDA_COUNT} .gcda profile files."
echo ""

# ---------------------------------------------------------------------------
# 3. PGOUse build — same build dir, same PGO_PROFILE_DIR absolute path
# ---------------------------------------------------------------------------

echo "[3/3] Configuring PGOUse build (reusing ${BUILD_DIR})..."
cmake -S "${REPO_ROOT}" -B "${BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE=PGOUse

echo "[3/3] Building PGO-optimised exchange_app and load_gen..."
cmake --build "${BUILD_DIR}" --target exchange_app load_gen -- -j"$(nproc)"
echo "[3/3] PGOUse build complete."
echo ""

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------

echo "=========================================="
echo " PGO BUILD COMPLETE"
echo "=========================================="
echo ""
echo " Final binaries:"
echo "   ${EXCHANGE_BIN}"
echo "   ${LOADGEN_BIN}"
echo ""
echo " To benchmark against the Release build:"
echo "   # Build Release baseline (if not already done):"
echo "   cmake -S ${REPO_ROOT} -B ${REPO_ROOT}/build -DCMAKE_BUILD_TYPE=Release"
echo "   cmake --build ${REPO_ROOT}/build -- -j\$(nproc)"
echo "   python3 ${REPO_ROOT}/tests/stress_test.py --duration 60 --clients ${CLIENTS} --rate ${RATE_PER_CLIENT} --output ${REPO_ROOT}/tests/results/baseline_release"
echo ""
echo "   # Swap in PGO binaries (stress_test.py looks in build/):"
echo "   cp ${EXCHANGE_BIN} ${REPO_ROOT}/build/exchange_app"
echo "   cp ${LOADGEN_BIN} ${REPO_ROOT}/build/load_gen"
echo "   python3 ${REPO_ROOT}/tests/stress_test.py --duration 60 --clients ${CLIENTS} --rate ${RATE_PER_CLIENT} --output ${REPO_ROOT}/tests/results/pgo_use"
echo "=========================================="
