#include "exchange/queue/spmc_broadcast_queue.hpp"
#include <gtest/gtest.h>
#include <thread>
#include <vector>

using namespace exchange::queue;

// ---------------------------------------------------------------------------
// Basic correctness
// ---------------------------------------------------------------------------

TEST(SpmcBroadcastQueueTest, CapacityMustBePowerOfTwo) {
    EXPECT_THROW((SpmcBroadcastQueue<int>(0)), std::invalid_argument);
    EXPECT_THROW((SpmcBroadcastQueue<int>(3)), std::invalid_argument);
    EXPECT_THROW((SpmcBroadcastQueue<int>(6)), std::invalid_argument);
    EXPECT_NO_THROW((SpmcBroadcastQueue<int>(8)));
    EXPECT_NO_THROW((SpmcBroadcastQueue<int>(1)));
}

TEST(SpmcBroadcastQueueTest, SingleConsumerBasicPushPop) {
    SpmcBroadcastQueue<int> queue(64);
    int cid = queue.register_consumer();

    EXPECT_TRUE(queue.push(42));

    int val = 0;
    EXPECT_TRUE(queue.pop(cid, val));
    EXPECT_EQ(val, 42);

    // Queue is now empty for this consumer.
    EXPECT_FALSE(queue.pop(cid, val));
}

TEST(SpmcBroadcastQueueTest, AllConsumersReceiveAllItems) {
    SpmcBroadcastQueue<int> queue(64);
    int c0 = queue.register_consumer();
    int c1 = queue.register_consumer();

    queue.push(1);
    queue.push(2);
    queue.push(3);

    int val = 0;
    // Consumer 0 sees all three items.
    EXPECT_TRUE(queue.pop(c0, val));
    EXPECT_EQ(val, 1);
    EXPECT_TRUE(queue.pop(c0, val));
    EXPECT_EQ(val, 2);
    EXPECT_TRUE(queue.pop(c0, val));
    EXPECT_EQ(val, 3);
    EXPECT_FALSE(queue.pop(c0, val));

    // Consumer 1 also sees all three items independently.
    EXPECT_TRUE(queue.pop(c1, val));
    EXPECT_EQ(val, 1);
    EXPECT_TRUE(queue.pop(c1, val));
    EXPECT_EQ(val, 2);
    EXPECT_TRUE(queue.pop(c1, val));
    EXPECT_EQ(val, 3);
    EXPECT_FALSE(queue.pop(c1, val));
}

TEST(SpmcBroadcastQueueTest, BackpressureWhenFull) {
    SpmcBroadcastQueue<int> queue(4);
    int cid = queue.register_consumer();

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));
    EXPECT_TRUE(queue.push(3));
    EXPECT_TRUE(queue.push(4));
    // Queue is full — push returns false.
    EXPECT_FALSE(queue.push(5));

    // Consumer reads one slot, freeing space.
    int val = 0;
    EXPECT_TRUE(queue.pop(cid, val));
    EXPECT_EQ(val, 1);

    // Now producer can push one more item.
    EXPECT_TRUE(queue.push(5));
}

TEST(SpmcBroadcastQueueTest, SlowConsumerBlocksProducer) {
    SpmcBroadcastQueue<int> queue(4);
    int c0 = queue.register_consumer();
    int c1 = queue.register_consumer();

    // Fill to capacity.
    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));
    EXPECT_TRUE(queue.push(3));
    EXPECT_TRUE(queue.push(4));
    EXPECT_FALSE(queue.push(5)); // Full — both consumers at position 0.

    // c0 drains its view — but c1 hasn't read anything yet.
    int val = 0;
    queue.pop(c0, val);
    queue.pop(c0, val);
    queue.pop(c0, val);
    queue.pop(c0, val);

    // Producer still blocked because c1 is at position 0 (4 behind producer).
    EXPECT_FALSE(queue.push(5));

    // c1 reads one item.
    queue.pop(c1, val);
    EXPECT_EQ(val, 1);

    // Now producer can push one item (min consumer is at position 1).
    EXPECT_TRUE(queue.push(5));
}

TEST(SpmcBroadcastQueueTest, RingBufferWrapAround) {
    SpmcBroadcastQueue<int> queue(4);
    int cid = queue.register_consumer();

    int val = 0;
    // Write and read 3 full rounds to exercise wrapping.
    for (int round = 0; round < 3; ++round) {
        for (int item = 1; item <= 4; ++item) {
            EXPECT_TRUE(queue.push(round * 10 + item));
        }
        for (int item = 1; item <= 4; ++item) {
            EXPECT_TRUE(queue.pop(cid, val));
            EXPECT_EQ(val, round * 10 + item);
        }
        EXPECT_FALSE(queue.pop(cid, val));
    }
}

TEST(SpmcBroadcastQueueTest, ConsumerStartsAfterRegistration) {
    SpmcBroadcastQueue<int> queue(64);

    // Push items before the second consumer registers.
    int c0 = queue.register_consumer();
    queue.push(100);
    queue.push(200);

    // c1 registers now — should NOT see the earlier items.
    int c1 = queue.register_consumer();
    queue.push(300);

    int val = 0;
    // c0 sees all three.
    EXPECT_TRUE(queue.pop(c0, val));
    EXPECT_EQ(val, 100);
    EXPECT_TRUE(queue.pop(c0, val));
    EXPECT_EQ(val, 200);
    EXPECT_TRUE(queue.pop(c0, val));
    EXPECT_EQ(val, 300);

    // c1 only sees the item pushed after its registration.
    EXPECT_TRUE(queue.pop(c1, val));
    EXPECT_EQ(val, 300);
    EXPECT_FALSE(queue.pop(c1, val));
}

// ---------------------------------------------------------------------------
// Concurrent tests
// ---------------------------------------------------------------------------

TEST(SpmcBroadcastQueueTest, ConcurrentSingleProducerSingleConsumer) {
    constexpr int NUM_ITEMS = 100000;
    SpmcBroadcastQueue<int> queue(1024);
    int cid = queue.register_consumer();

    std::thread producer([&]() {
        for (int i = 0; i < NUM_ITEMS; ++i) {
            while (!queue.push(i)) {
            }
        }
    });

    int count = 0;
    int expected = 0;
    while (count < NUM_ITEMS) {
        int val = 0;
        if (queue.pop(cid, val)) {
            EXPECT_EQ(val, expected);
            ++expected;
            ++count;
        }
    }

    producer.join();
    EXPECT_EQ(count, NUM_ITEMS);
}

TEST(SpmcBroadcastQueueTest, ConcurrentSingleProducerTwoConsumers) {
    constexpr int NUM_ITEMS = 50000;
    // Large enough that neither consumer can get more than one buffer behind.
    SpmcBroadcastQueue<int> queue(1024);
    int c0 = queue.register_consumer();
    int c1 = queue.register_consumer();

    std::thread producer([&]() {
        for (int i = 0; i < NUM_ITEMS; ++i) {
            while (!queue.push(i)) {
            }
        }
    });

    // Both consumers run concurrently and must each see all items in order.
    auto consume = [&](int consumer_id) {
        int expected = 0;
        while (expected < NUM_ITEMS) {
            int val = 0;
            if (queue.pop(consumer_id, val)) {
                EXPECT_EQ(val, expected);
                ++expected;
            }
        }
    };

    std::thread t_c0(consume, c0);
    std::thread t_c1(consume, c1);

    producer.join();
    t_c0.join();
    t_c1.join();
}
