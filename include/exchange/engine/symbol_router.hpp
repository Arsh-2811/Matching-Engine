#pragma once
#include "exchange/engine/order_book.hpp"
#include <array>
#include <cstdint>

namespace exchange::engine {

static constexpr int MAX_SYMBOLS = 64;

class SymbolRouter {
  private:
    std::array<OrderBook *, MAX_SYMBOLS> books_{};

  public:
    void register_book(uint16_t symbol_id, OrderBook *book) {
        if (symbol_id < MAX_SYMBOLS) {
            books_[symbol_id] = book;
        }
    }

    [[nodiscard]] auto get_book(uint16_t symbol_id) const -> OrderBook * {
        if (symbol_id < MAX_SYMBOLS) {
            return books_[symbol_id];
        }
        return nullptr;
    }
};

} // namespace exchange::engine
