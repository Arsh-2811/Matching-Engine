# Matching Engine

A low-latency financial matching engine with price-time priority order matching.

Two branches reflect two milestones:
- **`master`** — clean baseline engine (no cancel/ACK, no maker fills): **14.6M/s** unlimited, **0.05ms** P50 at 1.7M/client
- **`feat/cancel-ack-maker-fills`** — full feature set (cancel/ACK + maker fill notifications): **7.1M/s** unlimited, **0.34ms** P50 at 1.4M/client

## Build & Run

```bash
# Build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build

# Run (two terminals)
./build/exchange_app                   # engine, listens on :9000
./build/load_gen                       # 4 clients at sweet-spot rate (default: 1700k/client)
./build/load_gen 4 0                   # 4 clients, unlimited rate
./build/load_gen 4 1700000             # 4 clients, explicit 1.7M orders/sec each

# Tests (64 tests on master)
ctest --test-dir build

# Stress test / parameter sweep / rate sweep
python3 tests/stress_test.py --duration 60 --clients 4 --rate 1700000
python3 tests/param_sweep.py --duration 20
python3 tests/rate_sweep_plot.py --duration 12   # plots throughput vs latency curve
```

## Architecture (master — baseline)

```text
Load Generator (4 client threads, 1700k orders/sec/client default)
    │  TCP, SBE binary, rdtsc() timestamps, BATCH_SIZE=64 orders/send()
    │  Token-bucket rate limiter per thread
GatewayWorker (cores 2,3) — epoll edge-triggered, TCP_NODELAY, 8MB compaction buffer
    │
MpscQueue [inbound] — lock-free ring buffer, 2M slots
    │
MatchingEngine (core 1) — single consumer, batch drain (64/iter), no syscalls on hot path
    │  OrderBook: price-time priority, uint32_t slab indices (32B/node), prefetch
    │  process_order() → matched_qty
SpmcBroadcastQueue [outbound] — 1M slots, cached min-cursor fast path
    │
MetricsThread (core 3) — LatencyHistogram 512 buckets (P50/P99/P99.9), 1s prints
```

## Performance (i5-9300H, 2.4 GHz, 4 cores, no isolcpus)

### Rate sweep — master (BATCH=64, Q=2M, 4 clients)

| Rate/client | Total | Mean Tput | P50 Lat | Notes |
|---|---|---|---|---|
| unlimited | — | 14.59M/s | 145ms | queue saturated |
| 2000k | 8M/s | 7.98M/s | 5.68ms | over the cliff |
| **1700k ★** | **6.8M/s** | **5.63M/s** | **0.05ms** | **sweet spot** |
| 1400k | 5.6M/s | 5.60M/s | 0.05ms | — |
| 1000k | 4M/s | 4.00M/s | 0.06ms | — |

★ = highest rate before the latency cliff. Default for `./build/load_gen`.

The latency cliff occurs when input rate matches engine capacity and the MpscQueue fills permanently. Every order then waits behind ~2M others = ~145ms queue delay. Rate-limit to stay below the cliff.

Unit-test benchmarks (no TCP, deep book): ~6.45M orders/sec at 155 ns/order.

## Key Design Decisions

**OrderNode** (`include/exchange/engine/order_book_types.hpp`): 32 bytes — two nodes per 64-byte cache line. Keeping it at 32 bytes (not 48) halves cache misses during linked-list traversal in the matching hot path.

**SPMC cached_min_consumer_seq_** (`include/exchange/queue/spmc_broadcast_queue.hpp`): A non-atomic field on the producer's cache line. Avoids a cross-core acquire load (~50ns) on every `push()` in the common case (queue not full).

**BATCH_SIZE=64**: One `send()` syscall per 64 orders. Reduces syscall overhead from O(rate) to O(rate/64). Below BATCH=16 the load gen becomes syscall-bound and cannot sustain the target rate.

**Token-bucket rate limiter** (`tests/load_generator.cpp:35`): TSC-based, no syscalls or sleeps. CPU spins between batches on `CPU_PAUSE()`. The rate limit is what keeps the inbound queue empty and latency low.
