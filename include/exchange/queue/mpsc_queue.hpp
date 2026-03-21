#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <stdexcept>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define QUEUE_CPU_PAUSE() _mm_pause()
#elif defined(__aarch64__) || defined(_M_ARM64)
#define QUEUE_CPU_PAUSE() __asm__ volatile("yield" ::: "memory")
#else
#define QUEUE_CPU_PAUSE()
#endif

namespace exchange::queue {
constexpr std::size_t CACHE_LINE_SIZE = 64;

template <typename T> class MpscQueue {
  private:
    struct alignas(CACHE_LINE_SIZE) Slot {
        T data;
        std::atomic<bool> turn{false};
    };

    // NOLINTNEXTLINE(modernize-avoid-c-arrays)
    std::unique_ptr<Slot[]> buffer_;
    std::size_t capacity_mask_;

    alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> write_index_{0};
    alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> read_index_{0};

  public:
    explicit MpscQueue(std::size_t capacity) {
        if ((capacity == 0) || ((capacity & (capacity - 1)) != 0)) {
            throw std::invalid_argument("Capacity must be a positive number and power of 2");
        }
        // NOLINTNEXTLINE(modernize-avoid-c-arrays)
        buffer_ = std::make_unique<Slot[]>(capacity);
        capacity_mask_ = capacity - 1;
    }

    auto push(const T &item) -> bool {
        std::size_t w = write_index_.load(std::memory_order_relaxed);
        if (w - read_index_.load(std::memory_order_acquire) >= capacity_mask_ + 1) {
            return false;
        }
        std::size_t idx = write_index_.fetch_add(1, std::memory_order_relaxed);
        std::size_t pos = idx & capacity_mask_;
        // Wait for slot to be free (handles TOCTOU window under extreme contention)
        while (buffer_[pos].turn.load(std::memory_order_acquire)) {
            QUEUE_CPU_PAUSE();
        }
        buffer_[pos].data = item;
        buffer_[pos].turn.store(true, std::memory_order_release);
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

    [[nodiscard]] auto size() const -> std::size_t {
        return write_index_.load(std::memory_order_relaxed) -
               read_index_.load(std::memory_order_relaxed);
    }
};
} // namespace exchange::queue
