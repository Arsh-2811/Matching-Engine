#include "exchange/common/config.hpp"
#include "exchange/network/protocol.hpp"
#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace exchange::network;
using namespace std::chrono;
using namespace exchange::config;

constexpr size_t SINGLE_MSG_SIZE = sizeof(MsgHeader) + sizeof(SbeOrder);
constexpr size_t BATCH_BYTES = SINGLE_MSG_SIZE * load_gen::BATCH_SIZE;

void run_client(int client_id, int num_orders, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        return;
    }

    int opt = 1;
    if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
        perror("setsockopt TCP_NODELAY failed");
    }

    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    int attempts = 0;
    while (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        if (++attempts > 5) {
            std::cerr << "Client " << client_id << " failed to connect after 5 attempts.\n";
            close(sock);
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::vector<char> large_buffer(num_orders * SINGLE_MSG_SIZE);

    for (int i = 0; i < num_orders; ++i) {
        MsgHeader header{static_cast<uint16_t>(SINGLE_MSG_SIZE), MsgType::NEW_ORDER};
        SbeOrder order{.order_id = static_cast<uint64_t>(client_id * 10000000 + i + 1),
                       .price = 100,
                       .quantity = 10,
                       .side = 0, // BUY
                       .type = 0, // NEW
                       .timestamp_ns = 0};

        char *current_ptr = large_buffer.data() + (i * SINGLE_MSG_SIZE);
        std::memcpy(current_ptr, &header, sizeof(header));
        std::memcpy(current_ptr + sizeof(header), &order, sizeof(order));
    }

    int num_batches = num_orders / load_gen::BATCH_SIZE;
    for (int i = 0; i < num_batches; ++i) {
        char *batch_ptr = large_buffer.data() + (i * BATCH_BYTES);

        uint64_t batch_now =
            duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();

        for (int batch = 0; batch < load_gen::BATCH_SIZE; ++batch) {
            std::memcpy(batch_ptr + (batch * SINGLE_MSG_SIZE) + 26, &batch_now, sizeof(uint64_t));
        }

        if (send(sock, batch_ptr, BATCH_BYTES, 0) < 0) {
            perror("Send failed");
            break;
        }
    }

    close(sock);
}

int main(int argc, char *argv[]) {
    int num_clients = (argc > 1) ? std::stoi(argv[1]) : load_gen::NUM_CLIENTS;
    int orders_per_client = (argc > 2) ? std::stoi(argv[2]) : load_gen::ORDERS_PER_CLIENT;

    orders_per_client = (orders_per_client / load_gen::BATCH_SIZE) * load_gen::BATCH_SIZE;

    std::cout << "--- LOAD GENERATOR STARTING ---" << std::endl;
    std::cout << "Clients: " << num_clients << " | Orders per client: " << orders_per_client
              << std::endl;

    auto start_time = high_resolution_clock::now();

    std::vector<std::thread> clients;
    for (int i = 0; i < num_clients; ++i) {
        clients.emplace_back(run_client, i, orders_per_client, network::PORT);
    }

    for (auto &thread : clients) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    auto end_time = high_resolution_clock::now();
    duration<double> diff = end_time - start_time;

    std::cout << "--- LOAD GENERATOR FINISHED ---" << std::endl;
    std::cout << "Total Orders: " << (num_clients * orders_per_client) << std::endl;
    std::cout << "Total Time:   " << diff.count() << " seconds" << std::endl;
    std::cout << "Throughput:   " << (num_clients * orders_per_client) / diff.count()
              << " orders/sec" << std::endl;

    return 0;
}