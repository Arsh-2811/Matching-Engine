#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace exchange::engine {
template <typename T> class MemoryPool {
  private:
    std::vector<T> buffer_;
    uint32_t free_head_{T::NULL_IDX};
    std::size_t available_{0};

  public:
    explicit MemoryPool(std::size_t capacity) {
        if (capacity == 0) {
            throw std::invalid_argument("Pool Capacity must be greater than 0");
        }
        buffer_.resize(capacity);
        // Build the intrusive free list: each node points to the next one.
        for (std::size_t i = capacity; i > 0; --i) {
            buffer_[i - 1].next_idx = (i < capacity) ? static_cast<uint32_t>(i) : T::NULL_IDX;
        }
        free_head_ = 0;
        available_ = capacity;
    }

    MemoryPool(const MemoryPool &) = delete;
    MemoryPool &operator=(const MemoryPool &) = delete;
    MemoryPool(MemoryPool &&) = delete;
    MemoryPool &operator=(MemoryPool &&) = delete;
    ~MemoryPool() = default;

    [[nodiscard]] auto allocate() -> T * {
        if (free_head_ == T::NULL_IDX) {
            return nullptr;
        }
        T *obj = &buffer_[free_head_];
        free_head_ = obj->next_idx;
        obj->next_idx = T::NULL_IDX;
        --available_;
        return obj;
    }

    void deallocate(T *obj) {
        if (obj == nullptr) {
            return;
        }
        obj->next_idx = free_head_;
        free_head_ = index_of(obj);
        ++available_;
    }

    [[nodiscard]] auto available() const -> std::size_t { return available_; }

    // Direct index-based access into the contiguous slab.
    [[nodiscard]] auto get(uint32_t idx) -> T & { return buffer_[idx]; }
    [[nodiscard]] auto get(uint32_t idx) const -> const T & { return buffer_[idx]; }

    // Convert a slab pointer back to its uint32_t index (ptr must belong to this pool).
    [[nodiscard]] auto index_of(const T *ptr) const -> uint32_t {
        return static_cast<uint32_t>(ptr - buffer_.data());
    }
};
} // namespace exchange::engine
