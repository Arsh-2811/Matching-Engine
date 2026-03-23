#pragma once
#include "exchange/engine/types.hpp"
#include <cstdint>

namespace exchange::network {

enum class MsgType : uint16_t {
    NEW_ORDER = 1,
    CANCEL_ORDER = 2,
    MODIFY_ORDER = 3,
    IOC_ORDER = 4,
    FOK_ORDER = 5,
    ORDER_ACK = 6, // Server → Client
};

struct [[gnu::packed]] MsgHeader {
    uint16_t msg_size; // Includes Header + Payload
    MsgType type;
};

struct [[gnu::packed]] SbeOrder {
    uint64_t order_id; // For NEW: client ClOrdID. For CANCEL: ExchangeOrderID.
    uint64_t price;
    uint32_t quantity;
    uint8_t side;           // 0=Buy, 1=Sell
    uint8_t type;           // 0=New, 1=Cancel, 2=Modify
    uint64_t timestamp_tsc; // TSC value at order creation, set by the sender
};

// Sent by AckThread back to the client.
struct AckMsg {
    uint64_t exchange_order_id;
    uint64_t cl_ord_id;
    uint32_t matched_qty;
    uint32_t leaves_qty;
    uint8_t event_type; // engine::EventType value
    uint8_t _pad[7]{};
}; // 32 bytes

} // namespace exchange::network
