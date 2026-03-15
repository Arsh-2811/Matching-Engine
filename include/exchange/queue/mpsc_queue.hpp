#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <stdexcept>

namespace exchange::queue {
constexpr std::size_t CACHE_LINE_SIZE = 64;

template <typename T> class MpscQueue {
  private:
    struct alignas(CACHE_LINE_SIZE) Slot {
        T data;
        std::atomic<bool> turn{false};
    };

    std::unique_ptr<Slot[]> buffer_;
    std::size_t capacity_mask_;

    alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> write_index_{0};
    alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> read_index_{0};

  public:
    explicit MpscQueue(std::size_t capacity) {
        if ((capacity == 0) || ((capacity & (capacity - 1)) != 0)) {
            throw std::invalid_argument("Capacity must be a positive number and power of 2");
        }
        buffer_ = std::make_unique<Slot[]>(capacity);
        capacity_mask_ = capacity - 1;
    }

    auto push(const T &item) -> bool {
        std::size_t current_write = write_index_.load(std::memory_order_relaxed);
        do {
            if (current_write - read_index_.load(std::memory_order_acquire) >= capacity_mask_ + 1) {
                return false;
            }
        } while (!write_index_.compare_exchange_weak(current_write, current_write + 1,
                                                     std::memory_order_relaxed,
                                                     std::memory_order_relaxed));

        std::size_t actual_position = current_write & capacity_mask_;
        buffer_[actual_position].data = item;
        buffer_[actual_position].turn.store(true, std::memory_order_release);

        return true;
    }

    auto pop(T &item) -> bool {
        std::size_t current_read = read_index_.load(std::memory_order_relaxed);
        size_t actual_position = current_read & capacity_mask_;
        if (!buffer_[actual_position].turn.load(std::memory_order_acquire)) {
            return false;
        }
        item = buffer_[actual_position].data;
        buffer_[actual_position].turn.store(false, std::memory_order_relaxed);
        read_index_.store(current_read + 1, std::memory_order_release);
        return true;
    }
};
} // namespace exchange::queue