#pragma once

#include "exchange/engine/types.hpp"
#include "exchange/queue/mpsc_queue.hpp"
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
        static constexpr size_t BUFFER_SIZE = 8 * 1024 * 1024;
        std::vector<uint8_t> buffer;
        size_t read_idx{0};
        size_t write_idx{0};

        explicit ClientSession(int client_fd) : fd(client_fd) { buffer.resize(BUFFER_SIZE); }

        auto pending_bytes() -> size_t { return write_idx - read_idx; }
        auto available_space() -> size_t { return BUFFER_SIZE - write_idx; }
    };

    int listen_fd_{-1};
    int epoll_fd_{-1};
    std::unordered_map<int, ClientSession> sessions_;
    queue::MpscQueue<engine::Order> &incoming_queue_;

    void handle_client_data(int fd);
    void accept_new_clients();
    void set_non_blocking(int fd);
    void set_socket_options(int fd);

  public:
    explicit GatewayWorker(queue::MpscQueue<engine::Order> &queue) : incoming_queue_(queue) {}

    // To be called by the Gateway orchestrator inside a std::thread
    void run(int core_id, int port);
};

} // namespace exchange::network