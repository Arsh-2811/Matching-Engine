#pragma once

#include "exchange/network/worker.hpp"
#include <memory>
#include <thread>
#include <vector>

namespace exchange::network {

class Gateway {
  private:
    queue::MpscQueue<engine::Order> &queue_;
    int port_;
    std::vector<std::unique_ptr<GatewayWorker>> workers_;
    std::vector<std::thread> worker_threads_;

  public:
    Gateway(queue::MpscQueue<engine::Order> &q, int port, int num_workers)
        : queue_(q), port_(port) {
        for (int i = 0; i < num_workers; ++i) {
            workers_.push_back(std::make_unique<GatewayWorker>(queue_));
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
