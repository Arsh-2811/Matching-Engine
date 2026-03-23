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
                if (outbound_ != nullptr) {
                    MakerFillBuffer maker_buf;
                    ProcessResult result = (book != nullptr)
                                               ? book->process_order(buf[i], &maker_buf)
                                               : ProcessResult{};
                    OutboundEvent event;
                    event.order_id = buf[i].order_id;
                    event.entry_tsc = buf[i].timestamp_tsc;
                    event.process_tsc = process_tsc;
                    event.matched_qty = result.matched_qty;
                    event.leaves_qty = result.leaves_qty;
                    event.event_type = result.event_type;
                    event.client_fd = buf[i].client_fd;
                    event.cl_ord_id = buf[i].cl_ord_id;
                    // Drop on full — metrics miss a tick, engine never stalls.
                    if (!outbound_->push(event)) {
                        outbound_drops_.fetch_add(1, std::memory_order_relaxed);
                    }
                    // Push MAKER_FILLED events for each resting order filled by this aggressor.
                    for (uint8_t m = 0; m < maker_buf.count; ++m) {
                        const auto &mf = maker_buf.fills[m];
                        OutboundEvent mevent;
                        mevent.order_id = mf.order_id;
                        mevent.entry_tsc = 0; // no submission timestamp available for maker
                        mevent.process_tsc = process_tsc;
                        mevent.matched_qty = mf.matched_qty;
                        mevent.leaves_qty = mf.leaves_qty;
                        mevent.event_type = EventType::MAKER_FILLED;
                        mevent.client_fd = mf.client_fd;
                        mevent.cl_ord_id = mf.cl_ord_id;
                        if (!outbound_->push(mevent)) {
                            outbound_drops_.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                } else {
                    // Unit-test path: no outbound queue, still call process_order.
                    ProcessResult result =
                        (book != nullptr) ? book->process_order(buf[i]) : ProcessResult{};
                    (void)result;
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
