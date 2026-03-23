#pragma once

#include "exchange/common/config.hpp"
#include "exchange/engine/types.hpp"
#include "exchange/network/ack_routing_table.hpp"
#include "exchange/queue/mpsc_queue.hpp"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <sys/epoll.h>
#include <unordered_map>
#include <vector>

namespace exchange::network {

class GatewayWorker {
  private:
    struct ClientSession {
        int fd;
        static constexpr size_t BUFFER_SIZE = config::network::SESSION_BUFFER_SIZE;
        std::vector<uint8_t> buffer;
        size_t read_idx{0};
        size_t write_idx{0};
        std::vector<uint64_t> live_exchange_ids; // outstanding (unresolved) exchange order IDs

        explicit ClientSession(int client_fd) : fd(client_fd) { buffer.resize(BUFFER_SIZE); }

        [[nodiscard]] auto pending_bytes() const -> size_t { return write_idx - read_idx; }
        [[nodiscard]] auto available_space() const -> size_t { return BUFFER_SIZE - write_idx; }
    };

    int worker_id_;
    uint64_t id_base_;  // first ExchangeOrderID in this worker's partition
    uint64_t id_range_; // number of IDs in this worker's partition
    uint64_t local_counter_{0};

    int listen_fd_{-1};
    int epoll_fd_{-1};
    std::unordered_map<int, ClientSession> sessions_;
    queue::MpscQueue<engine::Order> &incoming_queue_;
    AckRoutingTable &routing_table_;
    std::atomic<bool> stop_flag_{false};

    [[nodiscard]] auto next_exchange_id() -> uint64_t {
        uint64_t id = id_base_ + (local_counter_ % id_range_) + 1;
        ++local_counter_;
        return id;
    }

    void handle_client_data(int sock_fd);
    void handle_disconnect(int sock_fd);
    void accept_new_clients();
    static void set_non_blocking(int sock_fd);
    static void set_socket_options(int sock_fd);

  public:
    GatewayWorker(queue::MpscQueue<engine::Order> &queue, AckRoutingTable &routing_table,
                  int worker_id, std::size_t max_orders_total, int num_workers)
        : worker_id_(worker_id),
          id_base_(static_cast<uint64_t>(worker_id) * (max_orders_total / num_workers)),
          id_range_(max_orders_total / num_workers), incoming_queue_(queue),
          routing_table_(routing_table) {}

    // To be called by the Gateway orchestrator inside a std::thread
    void run(int core_id, int port);

    void stop() { stop_flag_.store(true, std::memory_order_release); }
};

} // namespace exchange::network
