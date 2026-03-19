#pragma once

#include "exchange/engine/order_book.hpp"
#include "exchange/engine/types.hpp"
#include "exchange/queue/mpsc_queue.hpp"
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <x86intrin.h>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define ENGINE_CPU_PAUSE() _mm_pause()
#elif defined(__aarch64__) || defined(_M_ARM64)
#define ENGINE_CPU_PAUSE() __asm__ volatile("yield" ::: "memory")
#else
#define ENGINE_CPU_PAUSE()
#endif

inline auto rdtsc() -> uint64_t { return __rdtsc(); }

namespace exchange::engine {
class MatchingEngine {
  private:
    queue::MpscQueue<Order> &incoming_queue_;
    OrderBook &order_book_;
    std::atomic<bool> is_running_{false};
    std::thread engine_thread_;

    std::atomic<uint64_t> total_processed_{0};

    std::atomic<uint64_t> total_latency_ns_{0};
    std::atomic<uint64_t> max_latency_ns_{0};

    void run(int core_id) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        Order current_order;
        while (is_running_.load(std::memory_order_acquire)) {
            if (incoming_queue_.pop(current_order)) {
                // THE POISON PILL: If we receive ID 0, break the infinite loop!
                if (current_order.order_id == 0) {
                    break;
                }
                // uint64_t start_cycles = rdtsc();
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                uint64_t now_ns = static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + ts.tv_nsec;

                order_book_.process_order(current_order);

                // auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                //                std::chrono::high_resolution_clock::now().time_since_epoch())
                //                .count();
                if (current_order.timestamp_ns > 0) {
                    if (now_ns > current_order.timestamp_ns) {
                        uint64_t lat = now_ns - current_order.timestamp_ns;

                        total_latency_ns_.fetch_add(lat, std::memory_order_relaxed);

                        uint64_t current_max = max_latency_ns_.load(std::memory_order_relaxed);
                        while (lat > current_max &&
                               !max_latency_ns_.compare_exchange_weak(current_max, lat)) {
                        }
                    }
                }

                total_processed_.fetch_add(1, std::memory_order_relaxed);
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

    void start(int core_id) {
        if (!is_running_.load(std::memory_order_relaxed)) {
            is_running_.store(true, std::memory_order_release);
            engine_thread_ = std::thread(&MatchingEngine::run, this, core_id);
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

    [[nodiscard]] auto get_total_processed() const -> uint64_t { return total_processed_.load(); }

    uint64_t get_total_latency() { return total_latency_ns_.load(); }
    uint64_t get_max_latency() { return max_latency_ns_.exchange(0); }
};
} // namespace exchange::engine