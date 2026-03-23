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

    const OrderNode *head = book.get_head_node(bid_level);
    ASSERT_NE(head, nullptr);
    EXPECT_EQ(head->order_id, 1);
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
    EXPECT_EQ(book.get_head_node(ask_level), nullptr);
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

    EXPECT_EQ(book.get_head_node(book.get_bids().get_level(150))->order_id, 1);

    book.process_order({1, 150, 200, Side::BUY, OrderType::MODIFY});

    EXPECT_EQ(book.get_head_node(book.get_bids().get_level(150))->order_id, 2);

    EXPECT_EQ(book.get_bids().get_level(150)->total_quantity, 300);
}

// ---------------------------------------------------------------------------
// Index-based linked-list correctness tests
// These exercise the new uint32_t slab-index traversal paths directly.
// ---------------------------------------------------------------------------

// Matching must consume resting orders head-first (price-time priority).
// After the head fill, head_idx advances and the new head's prev_idx must
// be NULL_IDX — otherwise a later cancel of that node would corrupt the list.
TEST_F(OrderBookTest, MatchingAdvancesHeadAndClearsPrevIdx) {
    // Three resting sells at the same price.
    book.process_order({10, 150, 100, Side::SELL, OrderType::NEW});
    book.process_order({20, 150, 100, Side::SELL, OrderType::NEW});
    book.process_order({30, 150, 100, Side::SELL, OrderType::NEW});
    EXPECT_EQ(book.get_available_capacity(), 997);

    // Buy exactly enough to consume the first maker only.
    book.process_order({99, 150, 100, Side::BUY, OrderType::NEW});

    // One node freed; two remain.
    EXPECT_EQ(book.get_available_capacity(), 998);

    PriceLevel *ask = book.get_asks().get_level(150);
    EXPECT_EQ(ask->total_quantity, 200);

    // New head must be order 20 with a cleared prev link.
    const OrderNode *head = book.get_head_node(ask);
    ASSERT_NE(head, nullptr);
    EXPECT_EQ(head->order_id, 20);
    EXPECT_EQ(head->prev_idx, OrderNode::NULL_IDX);
}

// Cancelling the middle node of a three-element list must leave the
// remaining two nodes properly linked to each other.
TEST_F(OrderBookTest, CancelMiddleNodePreservesListIntegrity) {
    book.process_order({10, 150, 100, Side::SELL, OrderType::NEW});
    book.process_order({20, 150, 100, Side::SELL, OrderType::NEW});
    book.process_order({30, 150, 100, Side::SELL, OrderType::NEW});

    // Cancel the middle order.
    book.process_order({20, 150, 0, Side::SELL, OrderType::CANCEL});

    EXPECT_EQ(book.get_available_capacity(), 998);

    PriceLevel *ask = book.get_asks().get_level(150);
    EXPECT_EQ(ask->total_quantity, 200); // only 10 and 30 remain

    // Sweep with a large buy — must fill 10 then 30, skipping the cancelled 20.
    book.process_order({99, 160, 200, Side::BUY, OrderType::NEW});

    EXPECT_EQ(book.get_available_capacity(), 1000);
    EXPECT_EQ(ask->total_quantity, 0);
}

// Cancelling the tail node must update tail_idx and leave the remaining
// list intact (head unchanged, second node's next_idx becomes NULL_IDX).
TEST_F(OrderBookTest, CancelTailNodeUpdatesTailIdx) {
    book.process_order({10, 150, 100, Side::SELL, OrderType::NEW});
    book.process_order({20, 150, 100, Side::SELL, OrderType::NEW});

    // Cancel the tail.
    book.process_order({20, 150, 0, Side::SELL, OrderType::CANCEL});

    EXPECT_EQ(book.get_available_capacity(), 999);

    PriceLevel *ask = book.get_asks().get_level(150);
    EXPECT_EQ(ask->total_quantity, 100);
    EXPECT_EQ(ask->tail_idx, ask->head_idx); // only one node left

    const OrderNode *head = book.get_head_node(ask);
    ASSERT_NE(head, nullptr);
    EXPECT_EQ(head->order_id, 10);
    EXPECT_EQ(head->next_idx, OrderNode::NULL_IDX);
}

// ---------------------------------------------------------------------------
// IOC and FOK order type tests
// ---------------------------------------------------------------------------

TEST_F(OrderBookTest, IOCOrderFillsAndDiscardsRemainder) {
    // Resting sell for 50
    book.process_order({1, 150, 50, Side::SELL, OrderType::NEW});
    // IOC buy for 100 — only 50 available at or below 150
    uint32_t matched = book.process_order({2, 150, 100, Side::BUY, OrderType::IOC}).matched_qty;
    EXPECT_EQ(matched, 50); // filled 50
    // No resting buy should exist
    PriceLevel *bid = book.get_bids().get_level(150);
    EXPECT_TRUE(bid == nullptr || bid->total_quantity == 0);
}

TEST_F(OrderBookTest, IOCOrderNoLiquidityIsFullyCancelled) {
    // IOC with no resting orders — should cancel fully
    uint32_t matched = book.process_order({1, 150, 100, Side::BUY, OrderType::IOC}).matched_qty;
    EXPECT_EQ(matched, 0);
    PriceLevel *bid = book.get_bids().get_level(150);
    EXPECT_TRUE(bid == nullptr || bid->total_quantity == 0);
}

TEST_F(OrderBookTest, FOKOrderFullLiquidityFills) {
    // Resting sell for 100
    book.process_order({1, 150, 100, Side::SELL, OrderType::NEW});
    // FOK buy for 100 — enough liquidity
    uint32_t matched = book.process_order({2, 150, 100, Side::BUY, OrderType::FOK}).matched_qty;
    EXPECT_EQ(matched, 100);
}

TEST_F(OrderBookTest, FOKOrderInsufficientLiquidityIsCancelled) {
    // Resting sell for 50 only
    book.process_order({1, 150, 50, Side::SELL, OrderType::NEW});
    // FOK buy for 100 — insufficient, cancel entire order
    uint32_t matched = book.process_order({2, 150, 100, Side::BUY, OrderType::FOK}).matched_qty;
    EXPECT_EQ(matched, 0);
    // Resting sell should be UNCHANGED (FOK checked before touching book)
    PriceLevel *ask = book.get_asks().get_level(150);
    ASSERT_NE(ask, nullptr);
    EXPECT_EQ(ask->total_quantity, 50);
}