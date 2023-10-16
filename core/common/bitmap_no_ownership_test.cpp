#include "common/bitmap_no_ownership.h"

#include <gtest/gtest.h>

namespace xyz::graph::core::common {

class BitmapNoOwnerShipTest : public ::testing::Test {
 protected:
  BitmapNoOwnerShipTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
};

TEST_F(BitmapNoOwnerShipTest, DefaultConstructor) {
  BitmapNoOwnerShip a;
  EXPECT_EQ(a.size(), 0);
  EXPECT_EQ(a.GetDataBasePointer(), nullptr);
}

TEST_F(BitmapNoOwnerShipTest, Constructor) {
  uint64_t* data = new uint64_t[10];
  BitmapNoOwnerShip a(10, data);
  EXPECT_EQ(a.size(), 10);
  EXPECT_EQ(a.GetDataBasePointer(), data);
}

}  // namespace xyz::graph::core::common