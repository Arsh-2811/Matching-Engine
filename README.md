# Matching Engine

A price-time priority order matching engine written in C++17. Built as an exercise in low-latency systems engineering — lock-free data structures, cache-line layout, CPU core affinity, and TSC-based timing.

**Measured on i5-9300H, no kernel isolation:**
- 5.2 M orders/sec end-to-end at 0.34 ms P50 latency (rate-limited, 4 TCP clients)
- 7.1 M orders/sec at queue saturation
- 23 M orders/sec in-process (no TCP, unit benchmark)

## Features

- Price-time priority limit order book
- Order types: limit, IOC, FOK, cancel
- ACK return path: exchange-assigned order IDs and fill events sent back to clients over TCP
- Maker fill notifications via a dedicated SPMC broadcast queue
- End-to-end latency measurement with `rdtsc()`
- Load generator with token-bucket rate limiting and a simple binary wire protocol

## Architecture

Five components pinned to dedicated CPU cores:

```
Load Generator  (separate process)
│  TCP · binary protocol · rdtsc() timestamp per order
│  Token-bucket rate limiter · BATCH_SIZE=64 orders per send()
│
GatewayWorker  (cores 2–3)
│  epoll edge-triggered · TCP_NODELAY · 8 MB compaction buffer
│  Assigns ExchangeOrderID; embeds cl_ord_id + client_fd into Order struct
│
MpscQueue [inbound]  2 M slots — lock-free ring buffer
│
MatchingEngine  (core 1)
│  Single consumer · no syscalls on hot path
│  OrderBook: price-time priority · 32-byte nodes · prefetch on traversal
│  process_order() → ProcessResult { matched_qty, leaves_qty, EventType }
│  Fills collected into MakerFillBuffer (stack-allocated, 8 slots)
│
├── SpmcBroadcastQueue [outbound]  1 M slots
│       MetricsThread  (core 3, critical consumer)
│         LatencyHistogram — 512 buckets, prints P50/P99/P99.9 every second
│       MarketDataThread  (core 3, best-effort stub)
│
└── SpmcBroadcastQueue [maker_fill]  64 K slots, zero critical consumers
        AckThread  (core 0, best-effort)
          Batches ACKs per client_fd
          Independent taker/maker flush counters
          send() responses back to load generator clients
```

## Performance

### Rate sweep  —  BATCH=64, Q=2M, 4 clients  (i5-9300H)

| Rate/client | Total rate | Throughput | P50 Latency | Notes |
|---|---|---|---|---|
| unlimited | — | 7.1 M/s | 23 ms | inbound queue saturated |
| 2000k | 8.0 M/s | 7.98 M/s | 5.68 ms | past the latency cliff |
| **1400k** | **5.6 M/s** | **5.20 M/s** | **0.34 ms** | sweet spot |
| 1000k | 4.0 M/s | 4.00 M/s | < 0.1 ms | — |

The latency cliff occurs when input rate exceeds engine capacity and the MpscQueue fills permanently. Each order then queues behind ~2 M others. Rate-limiting to ~70% of capacity keeps the queue near-empty and latency sub-millisecond.

### BATCH_SIZE impact  —  1400k/client, Q=2M

| BATCH | Throughput | P50 Latency | Notes |
|---|---|---|---|
| 64 | 5.20 M/s | 0.34 ms | optimal |
| 16 | 4.86 M/s | 0.16 ms | — |
| 8 | 3.46 M/s | 44 ms | load generator becomes syscall-bound |

Below BATCH=16 the load generator cannot sustain the target rate — it spends more time in `send()` syscalls than generating orders.

### Feature overhead  —  vs. baseline engine (no cancel/ACK, no maker fills)

| Feature | Throughput delta | Latency delta |
|---|---|---|
| Cancel/ACK return path | −30% at saturation | +0.13 ms P50 |
| Maker fill notifications | −1 M/s at saturation | +0.88 ms P50 (before fix) |
| Split flush-counter fix | none | −0.7 ms P50 (restored baseline) |

## Key Design Decisions

**32-byte OrderNode with cold-data sidecar**  (`include/exchange/engine/order_book_types.hpp`)

OrderNode is exactly 32 bytes — two nodes fit per 64-byte cache line. Routing fields (`cl_ord_id`, `client_fd`) are separated into a parallel `node_routing_[]` array indexed by pool slot. Hot matching data (price, linked-list pointers) stays in the compact node; routing data is fetched only at fill time.

Widening OrderNode to 48 bytes to include routing fields would cause it to straddle cache line boundaries, doubling cache misses during linked-list traversal. At 10 M nodes/sec this is a measurable regression.

**SPMC cached minimum consumer sequence**  (`include/exchange/queue/spmc_broadcast_queue.hpp`)

The producer caches the minimum consumer sequence in a non-atomic field on its own cache line. This avoids a cross-core acquire load (~50 ns on this CPU) on every `push()` in the common case (queue not full). The maker fill queue registers zero critical consumers, so `push()` never reads consumer cursors at all — push cost drops from ~105 ns to ~12 ns.

**Separate SPMC queue for maker fills**  (`include/exchange/engine/matching_engine.hpp`)

Routing maker fill events through the main outbound queue doubles the event rate seen by MetricsThread (the only critical consumer on that queue). At 50% fill rate and 10 M orders/sec, the queue fills and every `push()` triggers a 50 ns cross-core coherency access to get the minimum consumer sequence. This consumed ~500 ms/sec of engine CPU, halving throughput. A dedicated queue with zero critical consumers eliminates this entirely.

**Independent ACK flush counters**  (`include/exchange/engine/ack_thread.hpp`)

AckThread maintains separate `taker_events_since_flush` and `maker_events_since_flush` counters so a burst of maker fills cannot delay taker ACKs beyond the configured flush threshold.

**MakerFillBuffer declared inside the per-order loop**  (`include/exchange/engine/matching_engine.hpp`)

Declaring `MakerFillBuffer` inside the loop rather than hoisting it produces measurably better codegen — the compiler can treat the buffer as freshly initialized on each iteration and avoids dead-store suppression around the zero-initialization.

## Build & Run

```bash
# Release build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build

# Terminal 1 — start the exchange
./build/exchange_app

# Terminal 2 — start the load generator
./build/load_gen                   # 4 clients at 1400k orders/sec/client (default)
./build/load_gen 4 0               # unlimited rate
./build/load_gen 4 1400000         # explicit rate

# Run all tests (80 tests)
ctest --test-dir build

# Stress test and parameter sweep
python3 tests/stress_test.py --duration 60 --clients 4 --rate 1400000
python3 tests/param_sweep.py --duration 20
python3 tests/rate_sweep_plot.py --duration 12
```

## Hardware

All measurements on Intel i5-9300H (4 cores / 8 threads, no `isolcpus`). Core assignment: engine → 1, gateway → 2–3, metrics → 3, ACK → 0. Latency figures include OS scheduling noise from a non-isolated machine; steady-state P50 latency on uncontested runs is 0.03–0.06 ms.
