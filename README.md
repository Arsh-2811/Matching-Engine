# Matching Engine

A low-latency financial matching engine with price-time priority order matching. Achieves **15+ M orders/sec** throughput on a 4-core i5-9300H with sub-millisecond latency at sustainable rates.

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
    │  TCP, SBE binary, rdtsc() timestamps, BATCH_SIZE=64 orders/send()
GatewayWorkers (cores 2,3) — epoll edge-triggered, TCP_NODELAY
    │
MpscQueue [inbound] — lock-free ring buffer, 2M slots
    │
MatchingEngine (core 1) — single consumer, batch drain (64/iter), rdtsc() hot path
    │  OrderBook: price-time priority, uint32_t slab indices, prefetch
    │  process_order() → matched_qty
SpmcBroadcastQueue [outbound] — 1M slots, cached min-cursor fast path
    │
MetricsThread (core 3) — LatencyHistogram 512 buckets (P50/P99/P99.9), 1s prints
```

## Performance (i5-9300H, 2.4 GHz, 4 cores, no isolcpus)

With `-march=native -O3 -flto`, batch drain loop, intrusive slab free list, and sub-bucket histogram.

| Config | Mean Tput | P50 Tput | Mean Lat |
| --- | --- | --- | --- |
| **BATCH=64, Q=2M, unlimited** | **15.28M/s** | **15.60M/s** | high (queue backlog) |
| BATCH=32, Q=2M, unlimited | 11.27M/s | 11.19M/s | high |
| BATCH=16, Q=2M, unlimited | 11.60M/s | 11.74M/s | high |
| BATCH=8, Q=2M, unlimited | 6.61M/s | 6.83M/s | high |
| BATCH=64, Q=2M, 1400k/client | 5.57M/s | — | 0.99ms |
| BATCH=64, Q=2M, 1000k/client | 4.00M/s | — | 1.56ms |

At unlimited rate, latency is queueing delay, not engine processing time.
**Recommended**: BATCH=64 for max throughput.

Unit-test benchmarks (no TCP): deep-book ~6.45M/s at 155 ns/order; thin-book >5M/s.
