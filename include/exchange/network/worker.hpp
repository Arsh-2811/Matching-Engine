#pragma once

#include "exchange/engine/types.hpp"
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
        static constexpr size_t BUFFER_SIZE = 8UL * 1024UL * 1024UL;
        std::vector<uint8_t> buffer;
        size_t read_idx{0};
        size_t write_idx{0};

        explicit ClientSession(int client_fd) : fd(client_fd) { buffer.resize(BUFFER_SIZE); }

        [[nodiscard]] auto pending_bytes() const -> size_t { return write_idx - read_idx; }
        [[nodiscard]] auto available_space() const -> size_t { return BUFFER_SIZE - write_idx; }
    };

    int listen_fd_{-1};
    int epoll_fd_{-1};
    std::unordered_map<int, ClientSession> sessions_;
    queue::MpscQueue<engine::Order> &incoming_queue_;
    std::atomic<bool> stop_flag_{false};

    void handle_client_data(int sock_fd);
    void accept_new_clients();
    static void set_non_blocking(int sock_fd);
    static void set_socket_options(int sock_fd);

  public:
    explicit GatewayWorker(queue::MpscQueue<engine::Order> &queue) : incoming_queue_(queue) {}

    // To be called by the Gateway orchestrator inside a std::thread
    void run(int core_id, int port);

    void stop() { stop_flag_.store(true, std::memory_order_release); }
};

} // namespace exchange::network
