#pragma once

#include <cstddef>
#include <stdexcept>
#include <vector>

namespace exchange::engine {
template <typename T> class MemoryPool {
  private:
    std::vector<T> buffer_;
    std::vector<T *> free_list_;

  public:
    explicit MemoryPool(std::size_t capacity) {
        if (capacity == 0) {
            throw std::invalid_argument("Pool Capacity must be greater than 0");
        }
        buffer_.resize(capacity);
        free_list_.reserve(capacity);

        for (std::size_t i = capacity; i > 0; i--) {
            free_list_.push_back((&buffer_[i - 1]));
        }
    }

    MemoryPool(const MemoryPool &) = delete;
    MemoryPool &operator=(const MemoryPool &) = delete;
    MemoryPool(MemoryPool &&) = delete;
    MemoryPool &operator=(MemoryPool &&) = delete;
    ~MemoryPool() = default;

    [[nodiscard]] auto allocate() -> T * {
        if (free_list_.empty()) {
            return nullptr;
        }
        T *obj = free_list_.back();
        free_list_.pop_back();
        return obj;
    }

    void deallocate(T *obj) {
        if (obj != nullptr) {
            free_list_.push_back(obj);
        }
    }

    [[nodiscard]] auto available() const -> std::size_t { return free_list_.size(); }
};
} // namespace exchange::engine