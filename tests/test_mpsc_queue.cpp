#include "exchange/queue/mpsc_queue.hpp"
#include <cstddef>
#include <gtest/gtest.h>
#include <stdexcept>
#include <thread>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define CPU_PAUSE() _mm_pause()
#elif defined(__aarch64__) || defined(_M_ARM64)
#define CPU_PAUSE() __asm__ volatile("yield" ::: "memory")
#else
#define CPU_PAUSE()
#endif

using namespace exchange::queue;

TEST(MpscQueueTest, CapacityMustBePowerOfTwo) {
    EXPECT_NO_THROW(MpscQueue<int> valid_queue(1024));
    EXPECT_NO_THROW(MpscQueue<int> valid_queue(2));

    EXPECT_THROW(MpscQueue<int> valid_queue(1000), std::invalid_argument);
    EXPECT_THROW(MpscQueue<int> invalid_queue2(0), std::invalid_argument);
}

TEST(MpscQueueTest, PushUntilFull) {
    MpscQueue<int> queue(4);

    EXPECT_TRUE(queue.push(10));
    EXPECT_TRUE(queue.push(20));
    EXPECT_TRUE(queue.push(30));
    EXPECT_TRUE(queue.push(40));

    EXPECT_FALSE(queue.push(50));
}

TEST(MpscQueueTest, BasicPushAndPop) {
    MpscQueue<int> queue(4);
    int popped_item = 0;

    EXPECT_FALSE(queue.pop(popped_item));

    EXPECT_TRUE(queue.push(100));
    EXPECT_TRUE(queue.push(200));

    EXPECT_TRUE(queue.pop(popped_item));
    EXPECT_EQ(popped_item, 100);

    EXPECT_TRUE(queue.pop(popped_item));
    EXPECT_EQ(popped_item, 200);

    EXPECT_FALSE(queue.pop(popped_item));
}

TEST(MpscQueueTest, RingBufferWrapAround) {
    MpscQueue<int> queue(4);
    int popped_item = 0;

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));
    EXPECT_TRUE(queue.push(3));
    EXPECT_TRUE(queue.push(4));
    EXPECT_FALSE(queue.push(5));

    EXPECT_TRUE(queue.pop(popped_item));
    EXPECT_EQ(popped_item, 1);
    EXPECT_TRUE(queue.pop(popped_item));
    EXPECT_EQ(popped_item, 2);

    EXPECT_TRUE(queue.push(5));
    EXPECT_TRUE(queue.push(6));

    EXPECT_TRUE(queue.pop(popped_item));
    EXPECT_EQ(popped_item, 3);
    EXPECT_TRUE(queue.pop(popped_item));
    EXPECT_EQ(popped_item, 4);
    EXPECT_TRUE(queue.pop(popped_item));
    EXPECT_EQ(popped_item, 5);
    EXPECT_TRUE(queue.pop(popped_item));
    EXPECT_EQ(popped_item, 6);
}

TEST(MpscQueueTest, MultiThreadedStressTest) {
    constexpr std::size_t NUM_PRODUCERS = 4;
    constexpr std::size_t ITEMS_PER_PRODUCER = 100000;
    constexpr std::size_t TOTAL_ITEMS = NUM_PRODUCERS * ITEMS_PER_PRODUCER;
    constexpr std::size_t CAPACITY = 1024;

    MpscQueue<int> queue(CAPACITY);
    std::atomic<std::size_t> items_consumed{0};

    std::thread consumer([&]() {
        int item;
        while (items_consumed.load() < TOTAL_ITEMS) {
            if (queue.pop(item)) {
                items_consumed.fetch_add(1);
            }
        }
    });

    std::vector<std::thread> producers;
    for (std::size_t i = 0; i < NUM_PRODUCERS; i++) {
        producers.emplace_back([&queue, ITEMS_PER_PRODUCER]() {
            for (std::size_t j = 0; j < ITEMS_PER_PRODUCER; j++) {
                while (!queue.push(1)) {
                }
            }
        });
    }

    for (auto &producer : producers) {
        producer.join();
    }

    consumer.join();

    EXPECT_EQ(items_consumed.load(), TOTAL_ITEMS);
}

TEST(MpscQueueBenchmark, Throughput) {
    constexpr std::size_t NUM_PRODUCERS = 4;
    constexpr std::size_t MESSAGES_PER_PRODUCER = 5'000'000;
    constexpr std::size_t TOTAL_MESSAGES = NUM_PRODUCERS * MESSAGES_PER_PRODUCER;
    constexpr std::size_t CAPACITY = 65536;
    constexpr std::size_t NANOS_PER_SECOND = 1'000'000'000;

    MpscQueue<int> queue(CAPACITY);

    std::atomic<bool> start_flag{false};
    std::atomic<std::size_t> items_consumed{0};

    std::thread consumer([&]() {
        int item;
        while (!start_flag.load(std::memory_order_acquire)) {
            CPU_PAUSE();
        }

        while (items_consumed.load(std::memory_order_relaxed) < TOTAL_MESSAGES) {
            if (queue.pop(item)) {
                items_consumed.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    std::vector<std::thread> producers;
    for (std::size_t i = 0; i < NUM_PRODUCERS; ++i) {
        producers.emplace_back([&queue, &start_flag, MESSAGES_PER_PRODUCER]() {
            while (!start_flag.load(std::memory_order_acquire)) {
                CPU_PAUSE();
            }

            for (std::size_t j = 0; j < MESSAGES_PER_PRODUCER; ++j) {
                while (!queue.push(1)) {
                    // Spin-wait if the queue is full
                }
            }
        });
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    start_flag.store(true, std::memory_order_release);

    for (auto &producer : producers) {
        producer.join();
    }
    consumer.join();

    auto end_time = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> elapsed_seconds = end_time - start_time;
    double messages_per_second = TOTAL_MESSAGES / elapsed_seconds.count();
    double nanoseconds_per_message = (elapsed_seconds.count() * NANOS_PER_SECOND) / TOTAL_MESSAGES;

    std::cout << "\n=========================================\n";
    std::cout << "MPSC Queue Benchmark Results:\n";
    std::cout << "Total Messages: " << TOTAL_MESSAGES << "\n";
    std::cout << "Producers:      " << NUM_PRODUCERS << "\n";
    std::cout << "Time Elapsed:   " << elapsed_seconds.count() << " seconds\n";
    std::cout << "Throughput:     " << static_cast<std::size_t>(messages_per_second)
              << " msgs/sec\n";
    std::cout << "Latency/Msg:    " << nanoseconds_per_message << " ns\n";
    std::cout << "=========================================\n\n";

    EXPECT_EQ(items_consumed.load(), TOTAL_MESSAGES);
}