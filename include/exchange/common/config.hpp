#pragma once
#include <cstddef>
#include <cstdint>

namespace exchange::config {
namespace network {
constexpr int PORT = 9000;
constexpr size_t SESSION_BUFFER_SIZE = 8 * 1024 * 1024;
} // namespace network

namespace engine {
constexpr std::size_t QUEUE_CAPACITY = 2097152;
constexpr std::size_t MAX_ORDERS = 1000000;
constexpr uint64_t MIN_PRICE = 1;
constexpr uint64_t MAX_PRICE = 10000;
constexpr uint64_t TICK_SIZE = 1;

constexpr int ENGINE_CORE_ID = 1;
constexpr int GATEWAY_CORE_1 = 2;
constexpr int GATEWAY_CORE_2 = 3;
} // namespace engine

namespace load_gen {
constexpr int NUM_CLIENTS = 4;
constexpr int ORDERS_PER_CLIENT = 10000000;
constexpr int BATCH_SIZE = 64;
} // namespace load_gen
} // namespace exchange::config