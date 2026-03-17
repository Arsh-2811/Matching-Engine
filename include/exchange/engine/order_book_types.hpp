#pragma once

#include "exchange/engine/types.hpp"
#include <cstdint>

namespace exchange::engine {
struct PriceLevel;

struct OrderNode {
    uint64_t order_id{0};
    uint64_t price{0};
    uint32_t quantity{0};
    Side side{Side::BUY};

    PriceLevel *price_level{nullptr};
    OrderNode *prev{nullptr};
    OrderNode *next{nullptr};

    void reset() {
        order_id = 0;
        price = 0;
        quantity = 0;
        price_level = nullptr;
        prev = nullptr;
        next = nullptr;
    }
};

struct PriceLevel {
    uint64_t price{0};
    uint32_t total_quantity{0};

    OrderNode *head{nullptr};
    OrderNode *tail{nullptr};

    void append_order(OrderNode *order) {
        order->price_level = this;
        order->next = nullptr;
        order->prev = tail;

        if (tail != nullptr) {
            tail->next = order;
        } else {
            head = order;
        }

        tail = order;
        total_quantity += order->quantity;
    }

    void remove_order(OrderNode *order) {
        if (order->prev != nullptr) {
            order->prev->next = order->next;
        } else {
            head = order->next;
        }

        if (order->next != nullptr) {
            order->next->prev = order->prev;
        } else {
            tail = order->prev;
        }

        total_quantity -= order->quantity;

        order->prev = nullptr;
        order->next = nullptr;
        order->price_level = nullptr;
    }
};
} // namespace exchange::engine