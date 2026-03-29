#pragma once

#include <cstdint>

namespace exchange::engine {
constexpr std::size_t HALF_CACHE_LINE_SIZE = 32;

enum class Side : uint8_t { BUY, SELL };

enum class OrderType : uint8_t { NEW, CANCEL, MODIFY, IOC, FOK };

enum class EventType : uint8_t {
    NEW_ACK = 0,          // Order accepted and resting in book
    PARTIALLY_FILLED = 1, // Partially filled, remainder still resting
    FILLED = 2,           // Fully filled (taker)
    CANCELLED = 3,        // Cancel succeeded, or IOC/FOK with no fill
    CANCEL_REJECTED = 4,  // Cancel failed — order not found
    MAKER_FILLED = 5,     // Maker (resting) order fully or partially filled by an aggressor
};

// Order fields are laid out so the 40-byte hot matching data sits first,
// with routing fields (client_fd, cl_ord_id) appended for ACK use only.
struct Order {
    uint64_t order_id{0};
    uint64_t price{0};
    uint32_t quantity{0};
    Side side{Side::BUY};
    OrderType type{OrderType::NEW};
    uint16_t symbol_id{0};
    uint64_t timestamp_tsc{0};
    // Routing info embedded by GatewayWorker — copied through to OutboundEvent
    // so AckThread never touches the AckRoutingTable.
    int client_fd{-1}; // socket fd of the submitting client (-1 = no ACK needed)
    uint32_t _pad{0};
    uint64_t cl_ord_id{0}; // client's own order ID, echoed back in AckMsg
};
// 48 bytes total (no alignas — MpscQueue slot is independently 64-byte aligned)

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

// Info captured for each maker (resting) order filled during a match sweep.
// Collected by OrderBook::match_buy/match_sell into a MakerFillBuffer,
// then pushed as separate MAKER_FILLED OutboundEvents by the engine.
struct MakerFill {
    uint64_t order_id{0};
    uint64_t cl_ord_id{0};
    uint32_t matched_qty{0};
    uint32_t leaves_qty{0}; // 0 = fully filled, >0 = partially filled (still resting)
    int client_fd{-1};
    int32_t _pad{0};
}; // 32 bytes

static constexpr int MAX_MAKER_FILLS = 8;

struct MakerFillBuffer {
    MakerFill fills[MAX_MAKER_FILLS]{};
    uint8_t count{0};
    void clear() { count = 0; }
};

// ---------------------------------------------------------------------------
// Outbound events published by the engine to downstream consumers.
// Single-producer, broadcast semantics.
// 48 bytes: order_id(8) + entry_tsc(8) + process_tsc(8) + cl_ord_id(8) +
//           matched_qty(4) + leaves_qty(4) + client_fd(4) +
//           event_type(1) + _pad(3)
// ---------------------------------------------------------------------------
struct OutboundEvent {
    uint64_t order_id{0};
    uint64_t entry_tsc{0};
    uint64_t process_tsc{0};
    uint64_t cl_ord_id{0};
    uint32_t matched_qty{0};
    uint32_t leaves_qty{0};
    int client_fd{-1};
    EventType event_type{EventType::NEW_ACK};
    uint8_t _pad[3]{};
};

} // namespace exchange::engine
