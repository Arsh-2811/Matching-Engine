#pragma once

#include "exchange/engine/types.hpp"
#include "exchange/queue/mpsc_queue.hpp"
#include <cstdint>
#include <sys/epoll.h>
#include <unordered_map>
#include <vector>

namespace exchange::network {

class GatewayWorker {
  private:
    struct ClientSession {
        int fd;
        std::vector<uint8_t> buffer;

        explicit ClientSession(int client_fd) : fd(client_fd) { buffer.reserve(4096); }
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