#include <list>
#include <random>

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

TEST_F(BitmapTest, CountShouldReturnZero) {
  size_t k = 1 << 30;
  Bitmap bitmap(k);

  // After Clear all bits, return value of Count() should be 0.
  bitmap.Clear();
  EXPECT_EQ(0, bitmap.Count());
}

TEST_F(BitmapTest, CountShouldReturnSizeofBit) {
  size_t k = 1 << 30;
  Bitmap bitmap(k);

  // After fill all bits, return value of Count() should be bitmap.size().
  bitmap.Fill();
  EXPECT_EQ(bitmap.size(), bitmap.Count());
}

TEST_F(BitmapTest, CountShouldReturnAllKSetBits) {
  size_t k = 1 << 30;
  Bitmap bitmap(k);

  // Duplications should not impact the return value of Count(): return k.
  bitmap.Clear();
  for (size_t i = 0; i < k; i++) bitmap.SetBit(i);
  for (size_t i = 0; i < k % 3; i++) bitmap.SetBit(i);

  EXPECT_EQ(k, bitmap.Count());

  // Check k random visits.
  bitmap.Clear();
  k = random() % (1 << 8);
  std::list<size_t> l;
  while (l.size() < k) {
    l.push_back(rand() % k);
    l.sort();
    l.unique();
  }
  for (auto iter = l.begin(); iter != l.end(); iter++) bitmap.SetBit(*iter);
  EXPECT_EQ(k, bitmap.Count());
}

}  // namespace sics::graph::core::util
