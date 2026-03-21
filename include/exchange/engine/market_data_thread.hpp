#pragma once
#include "exchange/engine/types.hpp"
#include "exchange/queue/spmc_broadcast_queue.hpp"
#include <atomic>
#include <cstdio>
#include <pthread.h>
#include <thread>

namespace exchange::engine {

class MarketDataThread {
  private:
    queue::SpmcBroadcastQueue<OutboundEvent> &outbound_;
    std::atomic<bool> is_running_{false};
    std::thread thread_;
    int consumer_id_{-1};

    static constexpr uint64_t HEARTBEAT_INTERVAL = 1'000'000;

    void run(int core_id) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        uint64_t seq = 0;
        while (is_running_.load(std::memory_order_acquire)) {
            OutboundEvent event;
            if (!outbound_.pop(consumer_id_, event)) {
                std::this_thread::yield();
                continue;
            }
            ++seq;
            if (seq % HEARTBEAT_INTERVAL == 0) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
                std::printf("[MktData] seq=%llu\n", static_cast<unsigned long long>(seq));
                std::fflush(stdout);
            }
        }
    }

  public:
    explicit MarketDataThread(queue::SpmcBroadcastQueue<OutboundEvent> &outbound)
        : outbound_(outbound) {
        consumer_id_ = outbound_.register_consumer();
    }

    MarketDataThread(const MarketDataThread &) = delete;
    MarketDataThread &operator=(const MarketDataThread &) = delete;
    MarketDataThread(MarketDataThread &&) = delete;
    MarketDataThread &operator=(MarketDataThread &&) = delete;

    ~MarketDataThread() { stop(); }

    void start(int core_id) {
        if (!is_running_.load(std::memory_order_relaxed)) {
            is_running_.store(true, std::memory_order_release);
            thread_ = std::thread(&MarketDataThread::run, this, core_id);
        }
    }

    void stop() {
        is_running_.store(false, std::memory_order_release);
        if (thread_.joinable()) {
            thread_.join();
        }
    }
};

} // namespace exchange::engine
