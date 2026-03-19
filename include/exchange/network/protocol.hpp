#pragma once
#include <cstdint>

namespace exchange::network {
enum class MsgType : uint16_t { NEW_ORDER = 1, CANCEL_ORDER = 2, MODIFY_ORDER = 3 };

struct [[gnu::packed]] MsgHeader {
    uint16_t msg_size; // Includes Header + Payload
    MsgType type;
};

struct [[gnu::packed]] SbeOrder {
    uint64_t order_id;
    uint64_t price;
    uint32_t quantity;
    uint8_t side; // 0=Buy, 1=Sell
    uint8_t type; // 0=New, 1=Cancel, 2=Modify
    uint64_t timestamp_ns;
};
} // namespace exchange::network