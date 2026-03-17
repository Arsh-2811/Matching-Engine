#pragma once

#include "exchange/engine/order_book.hpp"
#include "exchange/engine/types.hpp"
#include "exchange/queue/mpsc_queue.hpp"
#include <atomic>
#include <thread>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define ENGINE_CPU_PAUSE() _mm_pause()
#elif defined(__aarch64__) || defined(_M_ARM64)
#define ENGINE_CPU_PAUSE() __asm__ volatile("yield" ::: "memory")
#else
#define ENGINE_CPU_PAUSE()
#endif

namespace exchange::engine {
class MatchingEngine {
  private:
    queue::MpscQueue<Order> &incoming_queue_;
    OrderBook &order_book_;
    std::atomic<bool> is_running_{false};
    std::thread engine_thread_;

    void run() {
        Order current_order;
        while (is_running_.load(std::memory_order_acquire)) {
            if (incoming_queue_.pop(current_order)) {
                // THE POISON PILL: If we receive ID 0, break the infinite loop!
                if (current_order.order_id == 0) {
                    break;
                }
                order_book_.process_order(current_order);
            } else {
                ENGINE_CPU_PAUSE();
            }
        }
        is_running_.store(false, std::memory_order_release);
    }

  public:
    explicit MatchingEngine(queue::MpscQueue<Order> &queue, OrderBook &book)
        : incoming_queue_(queue), order_book_(book) {}

    MatchingEngine(const MatchingEngine &) = delete;
    MatchingEngine &operator=(const MatchingEngine &) = delete;
    MatchingEngine(MatchingEngine &&) = delete;
    MatchingEngine &operator=(MatchingEngine &&) = delete;

    ~MatchingEngine() { stop(); }

    void start() {
        if (!is_running_.load(std::memory_order_relaxed)) {
            is_running_.store(true, std::memory_order_release);
            engine_thread_ = std::thread(&MatchingEngine::run, this);
        }
    }

    void stop() {
        if (is_running_.load(std::memory_order_acquire)) {
            is_running_.store(false, std::memory_order_release);
            if (engine_thread_.joinable()) {
                engine_thread_.join();
            }
        }
    }
};
} // namespace exchange::engine