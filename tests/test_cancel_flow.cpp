#include "exchange/engine/order_book.hpp"
#include "exchange/engine/types.hpp"
#include <gtest/gtest.h>

using namespace exchange::engine;

class CancelFlowTest : public ::testing::Test {
  protected:
    OrderBook book{1000, 100, 200, 1};
};

// 1. Cancel a resting buy → CANCELLED with correct leaves_qty
TEST_F(CancelFlowTest, CancelRestingBuyOrder) {
    book.process_order({1, 150, 100, Side::BUY, OrderType::NEW});
    auto result = book.process_order({1, 0, 0, Side::BUY, OrderType::CANCEL});
    EXPECT_EQ(result.event_type, EventType::CANCELLED);
    EXPECT_EQ(result.leaves_qty, 100u);
    EXPECT_EQ(result.matched_qty, 0u);
    EXPECT_EQ(book.get_available_capacity(), 1000);
}

// 2. Cancel a resting sell → CANCELLED
TEST_F(CancelFlowTest, CancelRestingSellOrder) {
    book.process_order({2, 160, 200, Side::SELL, OrderType::NEW});
    auto result = book.process_order({2, 0, 0, Side::SELL, OrderType::CANCEL});
    EXPECT_EQ(result.event_type, EventType::CANCELLED);
    EXPECT_EQ(result.leaves_qty, 200u);
}

// 3. Cancel non-existent order → CANCEL_REJECTED
TEST_F(CancelFlowTest, CancelNonExistentOrder) {
    auto result = book.process_order({99, 0, 0, Side::BUY, OrderType::CANCEL});
    EXPECT_EQ(result.event_type, EventType::CANCEL_REJECTED);
    EXPECT_EQ(result.leaves_qty, 0u);
    EXPECT_EQ(result.matched_qty, 0u);
}

// 4. Cancel fully filled order → CANCEL_REJECTED (already gone from book)
TEST_F(CancelFlowTest, CancelFullyFilledOrder) {
    book.process_order({1, 150, 100, Side::SELL, OrderType::NEW});
    book.process_order({2, 150, 100, Side::BUY, OrderType::NEW}); // fills order 1
    // order 1 is gone from book
    auto result = book.process_order({1, 0, 0, Side::SELL, OrderType::CANCEL});
    EXPECT_EQ(result.event_type, EventType::CANCEL_REJECTED);
}

// 5. Double cancel → second gets CANCEL_REJECTED
TEST_F(CancelFlowTest, DoubleCancelRejected) {
    book.process_order({1, 150, 100, Side::BUY, OrderType::NEW});
    auto r1 = book.process_order({1, 0, 0, Side::BUY, OrderType::CANCEL});
    EXPECT_EQ(r1.event_type, EventType::CANCELLED);
    auto r2 = book.process_order({1, 0, 0, Side::BUY, OrderType::CANCEL});
    EXPECT_EQ(r2.event_type, EventType::CANCEL_REJECTED);
}

// 6. Partial fill then cancel
TEST_F(CancelFlowTest, PartialFillThenCancel) {
    // Resting sell for 50
    book.process_order({1, 150, 50, Side::SELL, OrderType::NEW});
    // Buy for 100 — fills 50, rests 50
    auto new_result = book.process_order({2, 150, 100, Side::BUY, OrderType::NEW});
    EXPECT_EQ(new_result.event_type, EventType::PARTIALLY_FILLED);
    EXPECT_EQ(new_result.matched_qty, 50u);
    EXPECT_EQ(new_result.leaves_qty, 50u);

    // Cancel the remaining 50
    auto cancel_result = book.process_order({2, 0, 0, Side::BUY, OrderType::CANCEL});
    EXPECT_EQ(cancel_result.event_type, EventType::CANCELLED);
    EXPECT_EQ(cancel_result.leaves_qty, 50u);
    EXPECT_EQ(book.get_available_capacity(), 1000);
}

// 7. Cancel only order at a price level → level is empty
TEST_F(CancelFlowTest, CancelOnlyOrderEmptiesLevel) {
    book.process_order({1, 150, 100, Side::BUY, OrderType::NEW});
    book.process_order({1, 0, 0, Side::BUY, OrderType::CANCEL});
    PriceLevel *bid = book.get_bids().get_level(150);
    EXPECT_TRUE(bid == nullptr || bid->total_quantity == 0);
}

// 8. Cancel head of three-node level → next becomes new head
TEST_F(CancelFlowTest, CancelHeadOfThreeNodeLevel) {
    book.process_order({10, 150, 100, Side::BUY, OrderType::NEW});
    book.process_order({20, 150, 100, Side::BUY, OrderType::NEW});
    book.process_order({30, 150, 100, Side::BUY, OrderType::NEW});
    book.process_order({10, 0, 0, Side::BUY, OrderType::CANCEL});
    PriceLevel *bid = book.get_bids().get_level(150);
    EXPECT_EQ(bid->total_quantity, 200u);
    const OrderNode *head = book.get_head_node(bid);
    ASSERT_NE(head, nullptr);
    EXPECT_EQ(head->order_id, 20u);
    EXPECT_EQ(head->prev_idx, OrderNode::NULL_IDX);
}

// 9. Cancel tail of three-node level → prev becomes new tail
TEST_F(CancelFlowTest, CancelTailOfThreeNodeLevel) {
    book.process_order({10, 150, 100, Side::BUY, OrderType::NEW});
    book.process_order({20, 150, 100, Side::BUY, OrderType::NEW});
    book.process_order({30, 150, 100, Side::BUY, OrderType::NEW});
    book.process_order({30, 0, 0, Side::BUY, OrderType::CANCEL});
    PriceLevel *bid = book.get_bids().get_level(150);
    EXPECT_EQ(bid->total_quantity, 200u);
    // tail should now be order 20
    const OrderNode *head = book.get_head_node(bid);
    EXPECT_EQ(head->order_id, 10u);
    EXPECT_EQ(head->next_idx != OrderNode::NULL_IDX, true);
}

// 10. Cancel buy order — sell side completely unaffected
TEST_F(CancelFlowTest, CancelBuyDoesNotAffectSells) {
    book.process_order({1, 160, 50, Side::SELL, OrderType::NEW});
    book.process_order({2, 140, 100, Side::BUY, OrderType::NEW});
    book.process_order({2, 0, 0, Side::BUY, OrderType::CANCEL});
    PriceLevel *ask = book.get_asks().get_level(160);
    EXPECT_EQ(ask->total_quantity, 50u); // sell side untouched
}

// 11. New order that fully fills → FILLED, leaves_qty=0
TEST_F(CancelFlowTest, NewOrderFullyFilled) {
    book.process_order({1, 150, 100, Side::SELL, OrderType::NEW});
    auto result = book.process_order({2, 150, 100, Side::BUY, OrderType::NEW});
    EXPECT_EQ(result.event_type, EventType::FILLED);
    EXPECT_EQ(result.matched_qty, 100u);
    EXPECT_EQ(result.leaves_qty, 0u);
}

// 12. New order resting → NEW_ACK, matched_qty=0, leaves_qty=original_qty
TEST_F(CancelFlowTest, NewOrderResting) {
    auto result = book.process_order({1, 150, 100, Side::BUY, OrderType::NEW});
    EXPECT_EQ(result.event_type, EventType::NEW_ACK);
    EXPECT_EQ(result.matched_qty, 0u);
    EXPECT_EQ(result.leaves_qty, 100u);
}

// 13. IOC with liquidity → FILLED
TEST_F(CancelFlowTest, IOCWithLiquidityFilled) {
    book.process_order({1, 150, 100, Side::SELL, OrderType::NEW});
    auto result = book.process_order({2, 150, 100, Side::BUY, OrderType::IOC});
    EXPECT_EQ(result.event_type, EventType::FILLED);
    EXPECT_EQ(result.matched_qty, 100u);
    EXPECT_EQ(result.leaves_qty, 0u);
}

// 14. IOC with no liquidity → CANCELLED
TEST_F(CancelFlowTest, IOCNoLiquidityCancelled) {
    auto result = book.process_order({1, 150, 100, Side::BUY, OrderType::IOC});
    EXPECT_EQ(result.event_type, EventType::CANCELLED);
    EXPECT_EQ(result.matched_qty, 0u);
    EXPECT_EQ(result.leaves_qty, 0u);
}

// 15. FOK with sufficient liquidity → FILLED
TEST_F(CancelFlowTest, FOKSufficientLiquidity) {
    book.process_order({1, 150, 100, Side::SELL, OrderType::NEW});
    auto result = book.process_order({2, 150, 100, Side::BUY, OrderType::FOK});
    EXPECT_EQ(result.event_type, EventType::FILLED);
    EXPECT_EQ(result.matched_qty, 100u);
}

// 16. FOK insufficient liquidity → CANCELLED, book unchanged
TEST_F(CancelFlowTest, FOKInsufficientLiquidityCancelled) {
    book.process_order({1, 150, 50, Side::SELL, OrderType::NEW});
    auto result = book.process_order({2, 150, 100, Side::BUY, OrderType::FOK});
    EXPECT_EQ(result.event_type, EventType::CANCELLED);
    EXPECT_EQ(result.matched_qty, 0u);
    PriceLevel *ask = book.get_asks().get_level(150);
    EXPECT_EQ(ask->total_quantity, 50u); // book unchanged
}
