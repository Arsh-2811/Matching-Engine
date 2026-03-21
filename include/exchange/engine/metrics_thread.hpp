#pragma once

#include "exchange/common/config.hpp"
#include "exchange/common/tsc.hpp"
#include "exchange/engine/latency_histogram.hpp"
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
// reports throughput, latency percentiles, and trade volume to stdout.
//
// Runs on a dedicated thread (pinned to a core) but yields between empty polls
// to avoid starving any gateway worker sharing the same core.
//
// Time-check optimisation: steady_clock::now() (~15-20 ns each) is called
// only once every TIME_CHECK_INTERVAL loop iterations instead of on every
// spin. At 7 M events/s this reduces the overhead from ~105 ms/s to ~2 µs/s,
// freeing CPU time for the co-located gateway worker on the same core.
class MetricsThread {
  private:
    // How often (in loop iterations) to sample steady_clock. A power-of-2
    // allows a fast bitmask check instead of a modulo.
    static constexpr uint64_t TIME_CHECK_INTERVAL = uint64_t{1} << 16; // 65536

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
        LatencyHistogram lat_hist;

        auto last_print = std::chrono::steady_clock::now();
        uint64_t loop_count = 0;

        while (is_running_.load(std::memory_order_acquire)) {
            OutboundEvent event;
            if (outbound_queue_.pop(consumer_id_, event)) {
                if (event.type == OutboundEventType::ORDER_PROCESSED) {
                    ++order_count;
                    if (event.order_processed.matched_qty > 0) {
                        ++matched_orders;
                        total_matched_qty += event.order_processed.matched_qty;
                    }
                    // Record latency only when the sender provided a valid stamp.
                    if (event.order_processed.entry_tsc > 0 &&
                        event.order_processed.process_tsc > event.order_processed.entry_tsc) {
                        uint64_t lat =
                            event.order_processed.process_tsc - event.order_processed.entry_tsc;
                        total_latency_tsc += lat;
                        if (lat > max_latency_tsc) {
                            max_latency_tsc = lat;
                        }
                        lat_hist.record(lat);
                    }
                }
            } else {
                // Yield so the gateway worker sharing this core gets CPU time.
                std::this_thread::yield();
            }

            // Only call steady_clock::now() every TIME_CHECK_INTERVAL iterations
            // to avoid burning CPU time (~15 ns per call) on every spin.
            ++loop_count;
            if ((loop_count & (TIME_CHECK_INTERVAL - 1U)) != 0U) {
                continue;
            }

            auto now = std::chrono::steady_clock::now();
            auto elapsed_s =
                std::chrono::duration_cast<std::chrono::seconds>(now - last_print).count();
            if (elapsed_s >= 1) {
                if (order_count > 0) {
                    uint64_t lat_samples = lat_hist.sample_count;
                    double avg_lat_ns = (lat_samples > 0)
                                            ? (static_cast<double>(total_latency_tsc) /
                                               static_cast<double>(lat_samples)) *
                                                  ns_per_cycle_
                                            : 0.0;
                    double p50_lat_ns = lat_hist.percentile_ns(50.0, ns_per_cycle_);
                    double p99_lat_ns = lat_hist.percentile_ns(99.0, ns_per_cycle_);
                    double p999_lat_ns = lat_hist.percentile_ns(99.9, ns_per_cycle_);
                    double max_lat_ns = static_cast<double>(max_latency_tsc) * ns_per_cycle_;
                    double fill_rate = static_cast<double>(matched_orders) /
                                       static_cast<double>(order_count) * 100.0;
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
                    std::printf("Throughput: %llu orders/s"
                                " | Avg Latency: %.1f ns"
                                " | P50 Latency: %.1f ns"
                                " | P99 Latency: %.1f ns"
                                " | P99.9 Latency: %.1f ns"
                                " | Max Latency: %.1f ns"
                                " | Fill Rate: %.1f%%"
                                " | Volume: %llu\n",
                                static_cast<unsigned long long>(order_count), avg_lat_ns,
                                p50_lat_ns, p99_lat_ns, p999_lat_ns, max_lat_ns, fill_rate,
                                static_cast<unsigned long long>(total_matched_qty));
                    std::fflush(stdout);
                }
                // Reset snapshot counters for the next second.
                order_count = 0;
                total_latency_tsc = 0;
                max_latency_tsc = 0;
                matched_orders = 0;
                total_matched_qty = 0;
                lat_hist.reset();
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
