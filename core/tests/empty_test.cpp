#include <gtest/gtest.h>

namespace sics::graph::core::tests {

class SampleTest : public ::testing::Test {
 protected:
  SampleTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
};

TEST_F(SampleTest, SomeValuesAreAlwaysEqual) {
  EXPECT_EQ(1, 1);
  EXPECT_EQ(2, 2);
  EXPECT_EQ("miao", "miao");
}

}  // namespace sics::graph::core::common
