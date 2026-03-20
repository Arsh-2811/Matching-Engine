#pragma once

#include <cstdint>

namespace exchange::engine {
constexpr std::size_t HALF_CACHE_LINE_SIZE = 32;

enum class Side : uint8_t { BUY, SELL };

enum class OrderType : uint8_t { NEW, CANCEL, MODIFY };

struct alignas(HALF_CACHE_LINE_SIZE) Order {
    uint64_t order_id;
    uint64_t price;
    uint32_t quantity;
    Side side;
    OrderType type;
    uint64_t timestamp_tsc; // TSC value at order ingress, set by the load generator
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
// ---------------------------------------------------------------------------

enum class OutboundEventType : uint8_t { ORDER_PROCESSED };

struct OutboundEvent {
    struct OrderProcessedData {
        uint64_t order_id;
        uint64_t entry_tsc;   // timestamp_tsc from the incoming Order
        uint64_t process_tsc; // rdtsc() captured at the start of processing
        uint32_t matched_qty; // total quantity matched (0 if resting order)
    };

    OutboundEventType type;
    OrderProcessedData order_processed;
};

} // namespace exchange::engine
