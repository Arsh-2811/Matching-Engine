# Matching Engine

A low-latency financial matching engine with price-time priority order matching. Achieves **10+ M orders/sec** throughput on a 4-core i5-9300H with sub-millisecond latency at sustainable rates.

## Build & Run

```bash
# Build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build

# Run (two terminals)
./build/exchange_app          # engine, listens on :9000
./build/load_gen 4 1400000    # 4 clients, 1.4M orders/sec each

# Tests (58 tests)
ctest --test-dir build

# Stress test / parameter sweep
python3 tests/stress_test.py --duration 60 --clients 4 --rate 0
python3 tests/param_sweep.py --duration 30
```

## Architecture

``` text
Load Generator (4 client threads)
    │  TCP, SBE binary, rdtsc() timestamps, BATCH_SIZE=16 orders/send()
GatewayWorkers (cores 2,3) — epoll edge-triggered, TCP_NODELAY
    │
MpscQueue [inbound] — lock-free ring buffer, 2M slots
    │
MatchingEngine (core 1) — single consumer, rdtsc() hot path
    │  OrderBook: price-time priority, uint32_t slab indices, prefetch
    │  process_order() → matched_qty
SpmcBroadcastQueue [outbound] — 1M slots, cached min-cursor fast path
    │
MetricsThread (core 3) — LatencyHistogram (P50/P99/P99.9), 1s prints
```

## Performance (i5-9300H, 2.4 GHz, 4 cores, no isolcpus)

| Config | Mean Tput | P50 Tput | Mean Lat | P99 Lat |
| --- | --- | --- | --- | --- |
| BATCH=32, Q=2M, unlimited | **10.08M/s** | **10.77M/s** | high (queue backlog) | — |
| BATCH=64, Q=2M, unlimited | 9.83M/s | 10.41M/s | high | — |
| BATCH=16, Q=2M, unlimited | 6.54M/s* | 7.17M/s | high | — |
| BATCH=64, Q=2M, 1400k/client | 5.56M/s | — | 1.6ms | 13.8ms |
| BATCH=16, Q=2M, 1400k/client | 5.38M/s | — | 0.97ms | 2.1ms |
| BATCH=8, Q=2M, 1400k/client | 2.99M/s | — | **0.55ms** | **1.1ms** |

\* B16 mean suppressed by a single OS scheduling event (p50 = 7.17M/s is representative).
At unlimited rate, latency is queueing delay, not engine processing time.
**Recommended**: BATCH=32 for max throughput; BATCH=8 or BATCH=16 at 1400k/client for sub-ms latency.

Unit-test deep-book benchmark (no TCP): ~6.45M/s at 155 ns/order.
