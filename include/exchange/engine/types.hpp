#pragma once

#include <cstdint>

namespace exchange::engine {
constexpr std::size_t HALF_CACHE_LINE_SIZE = 32;

enum class Side : uint8_t { BUY, SELL };

enum class OrderType : uint8_t { NEW, CANCEL, MODIFY, IOC, FOK };

struct alignas(HALF_CACHE_LINE_SIZE) Order {
    uint64_t order_id;
    uint64_t price;
    uint32_t quantity;
    Side side;
    OrderType type;
    uint16_t symbol_id{0};     // uses the 2-byte padding slot between type and timestamp_tsc
    uint64_t timestamp_tsc{0}; // TSC value at order ingress, set by the load generator
};

struct Trade {
    uint64_t maker_order_id;
    uint64_t taker_order_id;
    uint64_t price;
    uint32_t quantity;
};

// ---------------------------------------------------------------------------
// Outbound events published by the engine to downstream consumers
// (e.g., MetricsThread). Single-producer, broadcast semantics.
// Flat layout: 32 bytes, no padding, fits one per half-cache-line.
// ---------------------------------------------------------------------------

struct OutboundEvent {
    uint64_t order_id;
    uint64_t entry_tsc;   // timestamp_tsc from the incoming Order
    uint64_t process_tsc; // rdtsc() captured at the start of processing
    uint32_t matched_qty; // total quantity matched (0 if resting order)
    uint32_t _pad{0};
};

} // namespace exchange::engine
