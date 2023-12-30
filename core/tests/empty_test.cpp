#include <gtest/gtest.h>

#include <fstream>

#include "core/common/multithreading/task.h"

namespace sics::graph::core::tests {

class SampleTest : public ::testing::Test {
 protected:
  SampleTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }

  std::string data_dir = TEST_DATA_DIR;
  std::string compress_file = data_dir + "/compress.txt";
};

TEST_F(SampleTest, SomeValuesAreAlwaysEqual) { EXPECT_EQ(1, 1); }

}  // namespace sics::graph::core::tests
