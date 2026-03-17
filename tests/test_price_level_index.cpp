#include "exchange/engine/price_level_index.hpp"
#include <cstdint>
#include <gtest/gtest.h>

using namespace exchange::engine;

TEST(PriceLevelIndexTest, InitializationAndBounds) {
    constexpr uint64_t min_price = 100;
    constexpr uint64_t max_price = 200;
    constexpr uint64_t tick_size = 10;

    PriceLevelIndex index(min_price, max_price, tick_size);

    EXPECT_EQ(index.get_level(90), nullptr);
    EXPECT_EQ(index.get_level(210), nullptr);
}

TEST(PriceLevelIndexTest, ExactMathematicalMapping) {
    constexpr uint64_t min_price = 1000;
    constexpr uint64_t max_price = 2000;
    constexpr uint64_t tick_size = 5;
    constexpr uint64_t mid_price = 1500;

    PriceLevelIndex index(min_price, max_price, tick_size);

    PriceLevel *level1 = index.get_level(min_price);
    ASSERT_NE(level1, nullptr);
    EXPECT_EQ(level1->price, 1000);

    PriceLevel *level2 = index.get_level(mid_price);
    ASSERT_NE(level2, nullptr);
    EXPECT_EQ(level2->price, 1500);

    PriceLevel *level3 = index.get_level(max_price);
    ASSERT_NE(level3, nullptr);
    EXPECT_EQ(level3->price, 2000);
}

TEST(PriceLevelIndexTest, ContiguousMemoryProof) {
    constexpr uint64_t min_price = 100;
    constexpr uint64_t max_price = 110;
    constexpr uint64_t tick_size = 1;
    constexpr uint64_t test_price = 101;

    PriceLevelIndex index(min_price, max_price, tick_size);

    PriceLevel *level_100 = index.get_level(min_price);
    PriceLevel *level_101 = index.get_level(test_price);

    auto addr_100 = reinterpret_cast<std::uintptr_t>(level_100);
    auto addr_101 = reinterpret_cast<std::uintptr_t>(level_101);

    EXPECT_EQ(addr_101 - addr_100, sizeof(PriceLevel));
}