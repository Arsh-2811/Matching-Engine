#pragma once

#include "exchange/common/config.hpp"
#include "exchange/network/ack_routing_table.hpp"
#include "exchange/network/worker.hpp"
#include <memory>
#include <thread>
#include <vector>

namespace exchange::network {

class Gateway {
  private:
    queue::MpscQueue<engine::Order> &queue_;
    AckRoutingTable &routing_table_;
    int port_;
    int num_workers_;
    std::vector<std::unique_ptr<GatewayWorker>> workers_;
    std::vector<std::thread> worker_threads_;

  public:
    Gateway(queue::MpscQueue<engine::Order> &q, AckRoutingTable &routing_table, int port,
            int num_workers)
        : queue_(q), routing_table_(routing_table), port_(port), num_workers_(num_workers) {
        for (int i = 0; i < num_workers_; ++i) {
            workers_.push_back(std::make_unique<GatewayWorker>(
                queue_, routing_table_, i, config::engine::MAX_ORDERS, num_workers_));
        }
    }

    void start(const std::vector<int> &core_ids) {
        for (size_t i = 0; i < workers_.size(); ++i) {
            int core_id = core_ids[i % core_ids.size()];
            worker_threads_.emplace_back(
                [this, i, core_id]() { workers_[i]->run(core_id, port_); });
        }
    }

    void stop() {
        for (auto &w : workers_) {
            w->stop();
        }
        for (auto &t : worker_threads_) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    ~Gateway() { stop(); }
};

} // namespace exchange::network
