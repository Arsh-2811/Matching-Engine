#include "exchange/engine/order_book.hpp"
#include <gtest/gtest.h>

using namespace exchange::engine;

TEST(OrderBookBasicTest, ProcessNewBuyOrder) {
    OrderBook book(100, 100, 200, 1);

    Order buy_order{
        .order_id = 1, .price = 150, .quantity = 50, .side = Side::BUY, .type = OrderType::NEW};

    book.process_order(buy_order);

    EXPECT_EQ(book.get_available_capacity(), 99);

    PriceLevel *bid_level = book.get_bids().get_level(150);
    ASSERT_NE(bid_level, nullptr);
    EXPECT_EQ(bid_level->total_quantity, 50);

    ASSERT_NE(bid_level->head, nullptr);
    EXPECT_EQ(bid_level->head->order_id, 1);
}

TEST(OrderBookBasicTest, ProcessCancelOrder) {
    OrderBook book(100, 100, 200, 1);

    Order sell_order{
        .order_id = 10, .price = 190, .quantity = 500, .side = Side::SELL, .type = OrderType::NEW};

    book.process_order(sell_order);
    EXPECT_EQ(book.get_available_capacity(), 99);

    Order cancel_order{
        .order_id = 10, .price = 0, .quantity = 0, .side = Side::SELL, .type = OrderType::CANCEL};

    book.process_order(cancel_order);

    EXPECT_EQ(book.get_available_capacity(), 100);

    PriceLevel *ask_level = book.get_asks().get_level(190);
    ASSERT_NE(ask_level, nullptr);
    EXPECT_EQ(ask_level->total_quantity, 0);
    EXPECT_EQ(ask_level->head, nullptr);
}

TEST(OrderBookBasicTest, ModifyIsCancelAndNew) {
    OrderBook book(100, 100, 200, 1);

    book.process_order({1, 150, 100, Side::BUY, OrderType::NEW});

    book.process_order({1, 150, 200, Side::BUY, OrderType::MODIFY});

    EXPECT_EQ(book.get_available_capacity(), 99);

    PriceLevel *bid_level = book.get_bids().get_level(150);
    EXPECT_EQ(bid_level->total_quantity, 200);
}

class OrderBookTest : public ::testing::Test {
  protected:
    OrderBook book{1000, 100, 200, 1};
};

TEST_F(OrderBookTest, ExactMatchConsumesBothSides) {

    book.process_order({1, 150, 100, Side::SELL, OrderType::NEW});
    EXPECT_EQ(book.get_available_capacity(), 999);

    book.process_order({2, 150, 100, Side::BUY, OrderType::NEW});

    EXPECT_EQ(book.get_available_capacity(), 1000);
    EXPECT_EQ(book.get_asks().get_level(150)->total_quantity, 0);
}

TEST_F(OrderBookTest, TakerSweepsMultiplePriceLevels) {
    book.process_order({1, 150, 50, Side::SELL, OrderType::NEW});
    book.process_order({2, 151, 50, Side::SELL, OrderType::NEW});
    book.process_order({3, 152, 50, Side::SELL, OrderType::NEW});

    EXPECT_EQ(book.get_available_capacity(), 997);

    book.process_order({4, 155, 120, Side::BUY, OrderType::NEW});

    EXPECT_EQ(book.get_asks().get_level(150)->total_quantity, 0);

    EXPECT_EQ(book.get_asks().get_level(151)->total_quantity, 0);

    EXPECT_EQ(book.get_asks().get_level(152)->total_quantity, 30);

    EXPECT_EQ(book.get_bids().get_level(155)->total_quantity, 0);

    EXPECT_EQ(book.get_available_capacity(), 999);
}

TEST_F(OrderBookTest, MakerIsLargerThanTaker) {
    book.process_order({1, 150, 5000, Side::BUY, OrderType::NEW});

    book.process_order({2, 150, 100, Side::SELL, OrderType::NEW});

    EXPECT_EQ(book.get_bids().get_level(150)->total_quantity, 4900);

    EXPECT_EQ(book.get_asks().get_level(150)->total_quantity, 0);
}

TEST_F(OrderBookTest, CancellationSafelyRecyclesMemory) {
    book.process_order({1, 150, 100, Side::BUY, OrderType::NEW});
    EXPECT_EQ(book.get_available_capacity(), 999);

    book.process_order({1, 0, 0, Side::BUY, OrderType::CANCEL}); // Price/Qty ignored for cancel

    EXPECT_EQ(book.get_available_capacity(), 1000);
    EXPECT_EQ(book.get_bids().get_level(150)->total_quantity, 0);
}

TEST_F(OrderBookTest, ModifyChangesPlaceInLine) {
    book.process_order({1, 150, 100, Side::BUY, OrderType::NEW}); // First in line
    book.process_order({2, 150, 100, Side::BUY, OrderType::NEW}); // Second in line

    EXPECT_EQ(book.get_bids().get_level(150)->head->order_id, 1);

    book.process_order({1, 150, 200, Side::BUY, OrderType::MODIFY});

    EXPECT_EQ(book.get_bids().get_level(150)->head->order_id, 2);

    EXPECT_EQ(book.get_bids().get_level(150)->total_quantity, 300);
}