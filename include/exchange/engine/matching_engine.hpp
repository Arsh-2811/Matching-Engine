#pragma once

#include "exchange/common/tsc.hpp"
#include "exchange/engine/order_book.hpp"
#include "exchange/engine/types.hpp"
#include "exchange/queue/mpsc_queue.hpp"
#include "exchange/queue/spmc_broadcast_queue.hpp"
#include <atomic>
#include <pthread.h>
#include <sched.h>
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
    // Optional outbound queue for ORDER_PROCESSED events. nullptr in unit-test
    // contexts — no events are emitted and all stat tracking is skipped.
    queue::SpmcBroadcastQueue<OutboundEvent> *outbound_;

    std::atomic<bool> is_running_{false};
    std::thread engine_thread_;

    void run(int core_id) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        Order current_order;
        while (is_running_.load(std::memory_order_acquire)) {
            if (incoming_queue_.pop(current_order)) {
                // Poison pill: order_id == 0 signals graceful shutdown.
                if (current_order.order_id == 0) {
                    break;
                }

                // Capture TSC before processing to measure queue-to-match latency.
                uint64_t process_tsc = rdtsc();
                uint32_t matched_qty = order_book_.process_order(current_order);

                if (outbound_ != nullptr) {
                    OutboundEvent event;
                    event.type = OutboundEventType::ORDER_PROCESSED;
                    event.order_processed.order_id = current_order.order_id;
                    event.order_processed.entry_tsc = current_order.timestamp_tsc;
                    event.order_processed.process_tsc = process_tsc;
                    event.order_processed.matched_qty = matched_qty;
                    // Drop on full — metrics miss a tick, engine never stalls.
                    outbound_->push(event);
                }
            } else {
                ENGINE_CPU_PAUSE();
            }
        }
        is_running_.store(false, std::memory_order_release);
    }

  public:
    explicit MatchingEngine(queue::MpscQueue<Order> &queue, OrderBook &book,
                            queue::SpmcBroadcastQueue<OutboundEvent> *outbound = nullptr)
        : incoming_queue_(queue), order_book_(book), outbound_(outbound) {}

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
};

} // namespace exchange::engine
