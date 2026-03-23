#pragma once

#include "exchange/common/tsc.hpp"
#include "exchange/engine/order_book.hpp"
#include "exchange/engine/symbol_router.hpp"
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
    static constexpr int DRAIN_BATCH = 64;

    queue::MpscQueue<Order> &incoming_queue_;
    OrderBook &order_book_;
    // Optional outbound queue for ORDER_PROCESSED events. nullptr in unit-test
    // contexts — no events are emitted and all stat tracking is skipped.
    queue::SpmcBroadcastQueue<OutboundEvent> *outbound_;
    // Optional symbol router for multi-instrument support. nullptr = single book.
    SymbolRouter *router_;

    std::atomic<bool> is_running_{false};
    std::atomic<uint64_t> outbound_drops_{0};
    std::thread engine_thread_;

    void run(int core_id) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        // NOLINTNEXTLINE(modernize-avoid-c-arrays)
        Order buf[DRAIN_BATCH];
        while (is_running_.load(std::memory_order_acquire)) {
            int num_drained = 0;
            while (num_drained < DRAIN_BATCH && incoming_queue_.pop(buf[num_drained])) {
                ++num_drained;
            }
            if (num_drained == 0) {
                ENGINE_CPU_PAUSE();
                continue;
            }
            for (int i = 0; i < num_drained; ++i) {
                if (buf[i].order_id == 0) {
                    is_running_.store(false, std::memory_order_release);
                    return;
                }
                uint64_t process_tsc = rdtsc();
                OrderBook *book =
                    (router_ != nullptr) ? router_->get_book(buf[i].symbol_id) : &order_book_;
                ProcessResult result =
                    (book != nullptr) ? book->process_order(buf[i]) : ProcessResult{};
                if (outbound_ != nullptr) {
                    OutboundEvent event;
                    event.order_id = buf[i].order_id;
                    event.entry_tsc = buf[i].timestamp_tsc;
                    event.process_tsc = process_tsc;
                    event.matched_qty = result.matched_qty;
                    event.leaves_qty = result.leaves_qty;
                    event.event_type = result.event_type;
                    // Drop on full — metrics miss a tick, engine never stalls.
                    if (!outbound_->push(event)) {
                        outbound_drops_.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        }
        is_running_.store(false, std::memory_order_release);
    }

  public:
    explicit MatchingEngine(queue::MpscQueue<Order> &queue, OrderBook &book,
                            queue::SpmcBroadcastQueue<OutboundEvent> *outbound = nullptr,
                            SymbolRouter *router = nullptr)
        : incoming_queue_(queue), order_book_(book), outbound_(outbound), router_(router) {}

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
        is_running_.store(false, std::memory_order_release);
        if (engine_thread_.joinable()) {
            engine_thread_.join();
        }
    }

    [[nodiscard]] auto outbound_drops() const -> uint64_t {
        return outbound_drops_.load(std::memory_order_relaxed);
    }
};

} // namespace exchange::engine
