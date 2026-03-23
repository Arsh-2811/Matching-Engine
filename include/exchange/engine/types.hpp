#pragma once

#include <cstdint>

namespace exchange::engine {
constexpr std::size_t HALF_CACHE_LINE_SIZE = 32;

enum class Side : uint8_t { BUY, SELL };

enum class OrderType : uint8_t { NEW, CANCEL, MODIFY, IOC, FOK };

enum class EventType : uint8_t {
    NEW_ACK = 0,          // Order accepted and resting in book
    PARTIALLY_FILLED = 1, // Partially filled, remainder still resting
    FILLED = 2,           // Fully filled
    CANCELLED = 3,        // Cancel succeeded, or IOC/FOK with no fill
    CANCEL_REJECTED = 4,  // Cancel failed — order not found
};

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

// Result returned by OrderBook::process_order().
struct ProcessResult {
    uint32_t matched_qty{0}; // qty filled by this order
    uint32_t leaves_qty{0};  // qty remaining in book (0 = fully resolved)
    EventType event_type{EventType::NEW_ACK};
};

// ---------------------------------------------------------------------------
// Outbound events published by the engine to downstream consumers.
// Single-producer, broadcast semantics.
// 40 bytes: order_id(8) + entry_tsc(8) + process_tsc(8) + matched_qty(4) +
//           leaves_qty(4) + event_type(1) + _pad(7)
// ---------------------------------------------------------------------------
struct OutboundEvent {
    uint64_t order_id;
    uint64_t entry_tsc;
    uint64_t process_tsc;
    uint32_t matched_qty;
    uint32_t leaves_qty;
    EventType event_type{EventType::NEW_ACK};
    uint8_t _pad[7]{};
};

} // namespace exchange::engine
