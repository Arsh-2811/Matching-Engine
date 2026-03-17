#pragma once

#include "exchange/engine/order_book_types.hpp"
#include "order_book_types.hpp"
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace exchange::engine {
class PriceLevelIndex {
  private:
    std::vector<PriceLevel> levels_;
    uint64_t min_price_;
    uint64_t max_price_;
    uint64_t tick_size_;

  public:
    explicit PriceLevelIndex(uint64_t min_price, uint64_t max_price, uint64_t tick_size)
        : min_price_(min_price), max_price_(max_price), tick_size_(tick_size) {

        if (tick_size_ == 0) {
            throw std::invalid_argument("Tick size cannot be 0");
        }
        if (min_price_ >= max_price_) {
            throw std::invalid_argument("Max price must be greater than min price.");
        }

        uint64_t num_levels = ((max_price_ - min_price_) / tick_size_) + 1;
        levels_.resize(num_levels);

        for (std::size_t i = 0; i < num_levels; ++i) {
            levels_[i].price = min_price_ + (i * tick_size_);
        }
    }

    PriceLevelIndex(const PriceLevelIndex &) = delete;
    PriceLevelIndex &operator=(const PriceLevelIndex &) = delete;

    [[nodiscard]] auto get_level(uint64_t price) -> PriceLevel * {
        if (price < min_price_ || price > max_price_) {
            return nullptr;
        }

        std::size_t index = (price - min_price_) / tick_size_;
        return &levels_[index];
    }
};
} // namespace exchange::engine