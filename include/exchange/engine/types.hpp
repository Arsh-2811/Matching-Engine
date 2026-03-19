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
    uint64_t timestamp_ns;
};

struct Trade {
    uint64_t maker_order_id;
    uint64_t taker_order_id;
    uint64_t price;
    uint32_t quantity;
};
} // namespace exchange::engine