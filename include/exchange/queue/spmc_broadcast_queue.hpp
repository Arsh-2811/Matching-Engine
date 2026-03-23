#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>

namespace exchange::queue {

static constexpr std::size_t SPMC_CACHE_LINE_SIZE = 64;
static constexpr std::size_t SPMC_DEFAULT_MAX_CONSUMERS = 8;

// Single-Producer Multiple-Consumer broadcast ring buffer.
//
// All registered consumers see ALL items (broadcast semantics). The producer
// is blocked (push returns false) when the slowest consumer is a full buffer
// behind. Consumers read independently at their own pace.
//
// Synchronisation contract:
//   producer: write data → release-store producer_seq_
//   consumer: acquire-load producer_seq_ → read data
// This establishes happens-before between the data write and data read.
//
// NOT thread-safe for multiple producers. Register all consumers before
// starting the producer thread.
template <typename T, std::size_t MaxConsumers = SPMC_DEFAULT_MAX_CONSUMERS>
class SpmcBroadcastQueue {
  private:
    // Each consumer cursor on its own cache line to prevent false sharing.
    struct alignas(SPMC_CACHE_LINE_SIZE) ConsumerSeq {
        std::atomic<uint64_t> value{0};
    };

    // NOLINTNEXTLINE(modernize-avoid-c-arrays)
    std::unique_ptr<T[]> buffer_;
    std::size_t capacity_;
    std::size_t capacity_mask_;

    alignas(SPMC_CACHE_LINE_SIZE) std::atomic<uint64_t> producer_seq_{0};
    // Producer-only cache: last known minimum consumer sequence.
    // Avoids cross-core acquire loads on every push(); refreshed only when the
    // cached value suggests the queue might be full.
    uint64_t cached_min_consumer_seq_{0};

    alignas(SPMC_CACHE_LINE_SIZE) std::array<ConsumerSeq, MaxConsumers> consumer_seqs_{};
    // Total registered consumers (critical + best-effort).
    alignas(SPMC_CACHE_LINE_SIZE) std::atomic<int> num_consumers_{0};
    // Only critical consumers exert backpressure on the producer.
    // Best-effort consumers must be registered AFTER all critical consumers.
    int num_critical_consumers_{0};

    [[nodiscard]] auto get_min_consumer_seq() const -> uint64_t {
        if (num_critical_consumers_ == 0) {
            // No critical consumers: no backpressure. Producer can always write.
            return producer_seq_.load(std::memory_order_relaxed);
        }
        uint64_t min_seq = consumer_seqs_[0].value.load(std::memory_order_acquire);
        for (int idx = 1; idx < num_critical_consumers_; ++idx) {
            uint64_t seq = consumer_seqs_[idx].value.load(std::memory_order_acquire);
            if (seq < min_seq) {
                min_seq = seq;
            }
        }
        return min_seq;
    }

  public:
    explicit SpmcBroadcastQueue(std::size_t capacity) {
        if ((capacity == 0) || ((capacity & (capacity - 1)) != 0)) {
            throw std::invalid_argument("Capacity must be a positive power of 2");
        }
        capacity_ = capacity;
        capacity_mask_ = capacity - 1;
        // NOLINTNEXTLINE(modernize-avoid-c-arrays)
        buffer_ = std::make_unique<T[]>(capacity);
    }

    SpmcBroadcastQueue(const SpmcBroadcastQueue &) = delete;
    SpmcBroadcastQueue &operator=(const SpmcBroadcastQueue &) = delete;
    SpmcBroadcastQueue(SpmcBroadcastQueue &&) = delete;
    SpmcBroadcastQueue &operator=(SpmcBroadcastQueue &&) = delete;
    ~SpmcBroadcastQueue() = default;

    // Register a critical consumer and return its consumer_id.
    // Critical consumers exert backpressure: push() returns false when any
    // critical consumer is a full buffer behind.
    // The consumer will only see items published AFTER this call.
    // NOT thread-safe — call before starting the producer.
    // IMPORTANT: all critical consumers must be registered before any
    // best-effort consumers (register_best_effort_consumer).
    [[nodiscard]] auto register_consumer() -> int {
        int consumer_id = num_consumers_.fetch_add(1, std::memory_order_relaxed);
        if (consumer_id >= static_cast<int>(MaxConsumers)) {
            throw std::runtime_error("Maximum number of consumers exceeded");
        }
        ++num_critical_consumers_;
        // Consumer starts from the current producer position.
        consumer_seqs_[consumer_id].value.store(producer_seq_.load(std::memory_order_acquire),
                                                std::memory_order_release);
        return consumer_id;
    }

    // Register a best-effort consumer. Its cursor is never checked for
    // backpressure — the producer is free to overwrite unread events.
    // If the consumer falls more than capacity_ slots behind, pop_best_effort()
    // will skip it ahead automatically (events are silently dropped).
    // Must be called AFTER all critical consumers have been registered.
    [[nodiscard]] auto register_best_effort_consumer() -> int {
        int consumer_id = num_consumers_.fetch_add(1, std::memory_order_relaxed);
        if (consumer_id >= static_cast<int>(MaxConsumers)) {
            throw std::runtime_error("Maximum number of consumers exceeded");
        }
        consumer_seqs_[consumer_id].value.store(producer_seq_.load(std::memory_order_acquire),
                                                std::memory_order_release);
        return consumer_id;
    }

    // Single-producer push. Returns false if all slots are full (slowest
    // consumer is a full buffer behind). The caller may spin-retry or drop.
    //
    // Fast path: check against the cached minimum consumer sequence first.
    // The cross-core acquire load (get_min_consumer_seq) is only performed
    // when the cached value indicates the queue might be full, amortising
    // the ~50 ns coherence penalty across many push() calls.
    auto push(const T &item) -> bool {
        uint64_t seq = producer_seq_.load(std::memory_order_relaxed);
        if (seq - cached_min_consumer_seq_ >= capacity_) {
            cached_min_consumer_seq_ = get_min_consumer_seq();
            if (seq - cached_min_consumer_seq_ >= capacity_) {
                return false;
            }
        }
        buffer_[seq & capacity_mask_] = item;
        // Release: makes the data write visible to any consumer that
        // subsequently acquires producer_seq_.
        producer_seq_.store(seq + 1, std::memory_order_release);
        return true;
    }

    // Consumer pop. Returns false if no new data is available.
    // Each consumer must use its own consumer_id obtained from register_consumer().
    auto pop(int consumer_id, T &item) -> bool {
        uint64_t consumer_seq = consumer_seqs_[consumer_id].value.load(std::memory_order_relaxed);
        // Acquire: pairs with the release-store in push(), ensuring the data
        // written before producer_seq_ was updated is visible here.
        if (consumer_seq >= producer_seq_.load(std::memory_order_acquire)) {
            return false;
        }
        item = buffer_[consumer_seq & capacity_mask_];
        consumer_seqs_[consumer_id].value.store(consumer_seq + 1, std::memory_order_release);
        return true;
    }

    // Best-effort pop. If this consumer has been lapped (the producer has
    // wrapped past it by >= capacity_ slots), the cursor is skipped forward to
    // the current producer position and false is returned (events are lost).
    // Safe to call concurrently with push() from the producer thread because
    // the safety invariant is the same as regular pop(): we only read slots
    // whose sequence number has been release-stored by the producer and that
    // the producer cannot yet overwrite (prod_seq - consumer_seq <= capacity_).
    auto pop_best_effort(int consumer_id, T &item) -> bool {
        uint64_t consumer_seq = consumer_seqs_[consumer_id].value.load(std::memory_order_relaxed);
        uint64_t prod_seq = producer_seq_.load(std::memory_order_acquire);
        if (consumer_seq >= prod_seq) {
            return false;
        }
        if (prod_seq - consumer_seq > capacity_) {
            // Lapped — skip to current producer position, lose old events.
            consumer_seqs_[consumer_id].value.store(prod_seq, std::memory_order_release);
            return false;
        }
        item = buffer_[consumer_seq & capacity_mask_];
        consumer_seqs_[consumer_id].value.store(consumer_seq + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] auto capacity() const -> std::size_t { return capacity_; }
    [[nodiscard]] auto num_consumers() const -> int {
        return num_consumers_.load(std::memory_order_relaxed);
    }
};

} // namespace exchange::queue
