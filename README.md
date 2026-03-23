# Matching Engine

A low-latency financial matching engine with price-time priority order matching.

Two branches reflect two milestones of development:
- **`master`** — clean baseline engine (no cancel/ACK, no maker fills): **14.6M/s** unlimited, **0.05ms** P50 at 1.7M/client
- **`feat/cancel-ack-maker-fills`** — full feature set (cancel/ACK + maker fill notifications): **7.1M/s** unlimited, **0.34ms** P50 at 1.4M/client

## Build & Run

```bash
# Build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build

# Run (two terminals)
./build/exchange_app                   # engine, listens on :9000
./build/load_gen                       # 4 clients at sweet-spot rate (default: 1400k/client)
./build/load_gen 4 0                   # 4 clients, unlimited rate
./build/load_gen 4 1400000             # 4 clients, explicit 1.4M orders/sec each

# Tests (80 tests)
ctest --test-dir build

# Stress test / parameter sweep / rate sweep
python3 tests/stress_test.py --duration 60 --clients 4 --rate 1400000
python3 tests/param_sweep.py --duration 20
python3 tests/rate_sweep_plot.py --duration 12   # plots throughput vs latency curve
```

## Architecture (feat/cancel-ack-maker-fills)

```text
Load Generator (4 client threads, 1400k orders/sec/client default)
    │  TCP, SBE binary, rdtsc() timestamps, BATCH_SIZE=64 orders/send()
    │  Token-bucket rate limiter per thread
GatewayWorker (cores 2,3) — epoll edge-triggered, TCP_NODELAY, 8MB compaction buffer
    │  Assigns ExchangeOrderID, writes cl_ord_id+client_fd into Order struct
MpscQueue [inbound] — lock-free ring buffer, 2M slots
    │
MatchingEngine (core 1) — single consumer, batch drain (64/iter), no syscalls on hot path
    │  OrderBook: price-time priority, uint32_t slab indices (32B/node), prefetch
    │  process_order() → ProcessResult{matched_qty, leaves_qty, EventType}
    │  match_buy/match_sell → MakerFillBuffer (stack, 8 slots) + node_routing_ sidecar
    │
    ├── SpmcBroadcastQueue [outbound] (1M slots) ──────────────────────┐
    │       MetricsThread (core 3, CRITICAL consumer)                  │
    │       LatencyHistogram 512 buckets (P50/P99/P99.9), prints 1/s   │
    │       MarketDataThread (core 3, BEST-EFFORT stub)                │
    │                                                                   │
    └── SpmcBroadcastQueue [maker_fill] (64K slots, 0 critical) ──────►AckThread (core 0)
                                                                        Batches ACKs per client_fd
                                                                        Independent flush counters
                                                                        per queue (taker vs maker)
                                                                        send() back to load gen
```

## Performance

### Throughput vs Latency — sweet spot per branch

| Branch | Config | Mean Tput | P50 Tput | P50 Lat |
|---|---|---|---|---|
| master | BATCH=64, Q=2M, unlimited | 14.59M/s | 14.61M/s | 145ms |
| master | BATCH=64, Q=2M, **1700k/client** ★ | 5.63M/s | 5.60M/s | **0.05ms** |
| feat/... | BATCH=64, Q=2M, unlimited | 7.13M/s | 7.34M/s | 23ms |
| feat/... | BATCH=64, Q=2M, **1400k/client** ★ | 5.20M/s | 5.58M/s | **0.34ms** |

★ = recommended operating point (highest rate before the latency cliff)

### Rate sweep insight

At unlimited rate the engine's MpscQueue fills up permanently — every order waits behind ~2M others at 14M/s = **145ms queue delay** (master). Rate-limit to 70–80% of capacity to keep the queue empty and achieve sub-millisecond latency.

The latency cliff on the feature branch hits at 1700k/client (vs 2000k on master) because cancel/ACK + maker fill overhead reduces engine headroom.

### Feature cost breakdown (feat branch vs master, rate-limited 1400k/client)

| Feature | Throughput impact | Latency impact |
|---|---|---|
| Cancel/ACK return path | −30% unlimited tput | +0.13ms P50 |
| Maker fill notifications | −1M/s unlimited tput | +0.88ms P50 (before fix) |
| Split ACK_BATCH counter fix | 0 | −0.7ms P50 (restored baseline) |

Unit-test benchmarks (no TCP, deep book): ~6.45M orders/sec at 155 ns/order.

## Key Design Decisions

**OrderNode layout** (`include/exchange/engine/order_book_types.hpp`): 32 bytes exactly — two nodes per 64-byte cache line. Routing data (`cl_ord_id`, `client_fd`) lives in a parallel `node_routing_` sidecar vector in OrderBook to keep the hot node compact.

**SPMC with zero critical consumers** (`include/exchange/queue/spmc_broadcast_queue.hpp`): The maker fill queue has no critical consumers. `push()` never reads consumer cursors (zero cross-core loads per push). Dropping from MpscQueue (105ns/push with cross-core acquire) to this structure was a 100× push-cost reduction.

**Independent ACK flush counters** (`include/exchange/engine/ack_thread.hpp`): AckThread maintains separate `taker_events_since_flush` and `maker_events_since_flush` counters so maker fill volume cannot inflate the taker ACK flush interval.

**Best-effort consumers**: AckThread and MarketDataThread register as best-effort SPMC consumers — they skip ahead when lapped and never block the engine producer.
