#pragma once
#include <cstddef>
#include <cstdint>

namespace exchange::config {

namespace network {
constexpr int PORT = 9000;
constexpr size_t SESSION_BUFFER_SIZE = 8ULL * 1024ULL * 1024ULL;
} // namespace network

namespace engine {
constexpr std::size_t QUEUE_CAPACITY = 2097152;          // 2M  — inbound MPSC queue
constexpr std::size_t OUTBOUND_QUEUE_CAPACITY = 1048576; // 1M  — outbound SPMC queue
constexpr std::size_t MAX_ORDERS = 1000000;
constexpr uint64_t MIN_PRICE = 1;
constexpr uint64_t MAX_PRICE = 10000;
constexpr uint64_t TICK_SIZE = 1;

constexpr int ENGINE_CORE_ID = 1;
constexpr int GATEWAY_CORE_1 = 2;
constexpr int GATEWAY_CORE_2 = 3;
constexpr int METRICS_CORE_ID = 3; // Shares core 3 with GATEWAY_CORE_2
constexpr int ACK_CORE_ID = 0;     // AckThread — main thread's core, otherwise idle

constexpr uint64_t TSC_CALIBRATION_MS = 50; // Duration for TSC frequency calibration
} // namespace engine

namespace load_gen {
constexpr int NUM_CLIENTS = 4;
constexpr int ORDERS_PER_CLIENT = 10000000;
constexpr int BATCH_SIZE = 64;

// Mid-price parameters for continuous matching load
constexpr uint64_t MID_PRICE_INIT = 1000; // Starting mid price
constexpr uint64_t MID_PRICE_MIN = 500;   // Minimum mid price (clamp)
constexpr uint64_t MID_PRICE_MAX = 1500;  // Maximum mid price (clamp)
constexpr uint64_t AGGRESSIVE_OFFSET = 2; // Buys at mid+offset, sells at mid-offset

// Default rate limit per client. 1400000 = sweet spot (cancel/ACK + maker fills):
// 5.2M/s total at ~0.34ms P50 — last rate before the latency cliff at 1700k/client.
// 0 = unlimited (max throughput, high latency).
constexpr uint64_t DEFAULT_RATE_LIMIT = 1400000;
} // namespace load_gen

} // namespace exchange::config
