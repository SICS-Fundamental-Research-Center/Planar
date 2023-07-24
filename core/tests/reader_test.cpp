#include "io/reader.h"

#include <gtest/gtest.h>

using sics::graph::core::io::Reader;

namespace sics::graph::core::test {

// The fixture for testing class LogTest
class ReaderTest : public ::testing::Test {
 protected:
  ReaderTest() = default;
  ~ReaderTest() override = default;
};

// TEST_F macro to define test cases using the ReaderTest fixture
TEST_F(ReaderTest, JudgeAdaptTest) {
  // Test the JudgeAdapt function of the Reader class
  // Instantiate a Reader object and call the JudgeAdapt function
  Reader reader("/Users/zhj/Projects/graph-systems/input/test_dir/config.yaml");
  bool result = reader.JudgeAdapt();
  ASSERT_TRUE(result);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

}  // namespace sics::graph::core::test
