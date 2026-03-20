#pragma once

#include "exchange/common/config.hpp"
#include "exchange/common/tsc.hpp"
#include "exchange/engine/types.hpp"
#include "exchange/queue/spmc_broadcast_queue.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <pthread.h>
#include <sched.h>
#include <thread>

namespace exchange::engine {

// Consumes OutboundEvents from the engine's broadcast queue and periodically
// reports throughput, latency, and trade volume to stdout.
//
// Runs on a dedicated thread (pinned to a core) but yields between empty polls
// to avoid starving any gateway worker sharing the same core.
class MetricsThread {
  private:
    queue::SpmcBroadcastQueue<OutboundEvent> &outbound_queue_;
    std::atomic<bool> is_running_{false};
    std::thread thread_;
    int consumer_id_{-1};
    double ns_per_cycle_{1.0};

    void run(int core_id) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        // Per-second snapshot accumulators — no atomics, single-threaded consumer.
        uint64_t order_count = 0;
        uint64_t total_latency_tsc = 0;
        uint64_t max_latency_tsc = 0;
        uint64_t matched_orders = 0;
        uint64_t total_matched_qty = 0;

        auto last_print = std::chrono::steady_clock::now();

        while (is_running_.load(std::memory_order_acquire)) {
            OutboundEvent event;
            if (outbound_queue_.pop(consumer_id_, event)) {
                if (event.type == OutboundEventType::ORDER_PROCESSED) {
                    ++order_count;
                    if (event.order_processed.matched_qty > 0) {
                        ++matched_orders;
                        total_matched_qty += event.order_processed.matched_qty;
                    }
                    // Only compute latency if the sender provided a valid TSC stamp.
                    if (event.order_processed.entry_tsc > 0 &&
                        event.order_processed.process_tsc > event.order_processed.entry_tsc) {
                        uint64_t lat =
                            event.order_processed.process_tsc - event.order_processed.entry_tsc;
                        total_latency_tsc += lat;
                        if (lat > max_latency_tsc) {
                            max_latency_tsc = lat;
                        }
                    }
                }
            } else {
                // Yield so the gateway worker sharing this core gets CPU time.
                std::this_thread::yield();
            }

            auto now = std::chrono::steady_clock::now();
            auto elapsed_s =
                std::chrono::duration_cast<std::chrono::seconds>(now - last_print).count();
            if (elapsed_s >= 1) {
                if (order_count > 0) {
                    double avg_lat_ns = (static_cast<double>(total_latency_tsc) /
                                         static_cast<double>(order_count)) *
                                        ns_per_cycle_;
                    double max_lat_ns = static_cast<double>(max_latency_tsc) * ns_per_cycle_;
                    double fill_rate = static_cast<double>(matched_orders) /
                                       static_cast<double>(order_count) * 100.0;
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
                    std::printf("Throughput: %llu orders/s | Avg Latency: %.1f ns"
                                " | Max Latency: %.1f ns | Fill Rate: %.1f%% | Volume: %llu\n",
                                static_cast<unsigned long long>(order_count), avg_lat_ns,
                                max_lat_ns, fill_rate,
                                static_cast<unsigned long long>(total_matched_qty));
                    std::fflush(stdout);
                }
                // Reset snapshot counters for the next second.
                order_count = 0;
                total_latency_tsc = 0;
                max_latency_tsc = 0;
                matched_orders = 0;
                total_matched_qty = 0;
                last_print = now;
            }
        }
    }

  public:
    // Calibrates TSC and registers as a consumer immediately at construction.
    // Must be constructed BEFORE the engine thread is started so that
    // register_consumer() runs before any push() calls.
    explicit MetricsThread(queue::SpmcBroadcastQueue<OutboundEvent> &outbound)
        : outbound_queue_(outbound) {
        ns_per_cycle_ = common::calibrate_tsc_ns_per_cycle(config::engine::TSC_CALIBRATION_MS);
        consumer_id_ = outbound_queue_.register_consumer();
    }

    MetricsThread(const MetricsThread &) = delete;
    MetricsThread &operator=(const MetricsThread &) = delete;
    MetricsThread(MetricsThread &&) = delete;
    MetricsThread &operator=(MetricsThread &&) = delete;

    ~MetricsThread() { stop(); }

    void start(int core_id) {
        if (!is_running_.load(std::memory_order_relaxed)) {
            is_running_.store(true, std::memory_order_release);
            thread_ = std::thread(&MetricsThread::run, this, core_id);
        }
    }

    void stop() {
        if (is_running_.load(std::memory_order_acquire)) {
            is_running_.store(false, std::memory_order_release);
            if (thread_.joinable()) {
                thread_.join();
            }
        }
    }
};

} // namespace exchange::engine
