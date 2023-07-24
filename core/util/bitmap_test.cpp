#include "bitmap.h"

#include <folly/FileUtil.h>
#include <folly/Format.h>
#include <folly/experimental/TestUtil.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace sics::graph::core::util {

// The fixture for testing class LogTest.
class BitmapTest : public ::testing::Test {
 protected:
  BitmapTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
};

TEST_F(BitmapTest, GetCountOfBitMapAsIntended) {
  Bitmap bitmap(65536000);
  bitmap.Clear();
  bitmap.SetBit(1234);
  bitmap.SetBit(1234);
  bitmap.SetBit(1313213);

  EXPECT_EQ(2, bitmap.Count());
}

}  // namespace sics::graph::core::util
