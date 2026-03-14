#include "add.hpp"
#include <gtest/gtest.h>

TEST(AddTest, Basic) { EXPECT_EQ(add(2, 3), 5); }

TEST(AddTest, Negative) { EXPECT_EQ(add(-1, 1), 0); }