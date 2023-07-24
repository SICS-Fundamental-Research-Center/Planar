#include "io/reader.h"
#include "data_structures/graph/serializable_immutable_csr.h"
#include <gtest/gtest.h>
#include <iostream>

using SerializedImmutableCSR =
    sics::graph::core::data_structures::graph::SerializedImmutableCSR;

namespace sics::graph::core::io {
namespace sics::graph::core::io {

// The fixture for testing class LogTest
class ReaderTest : public ::testing::Test {
 protected:
  ReaderTest() = default;
  ~ReaderTest() override = default;
};

// Test the JudgeAdapt function of the Reader class
// Instantiate a Reader object and call the JudgeAdapt function
TEST_F(ReaderTest, JudgeAdaptTest) {
  // Test with an existing config file
  Reader reader("/home/baiwc/workspace/graph-systems/input/test/config.yaml");
  bool result = reader.JudgeAdapt();
  ASSERT_TRUE(result);

  // Test with a non-existing config file
  Reader reader_non_exist("non_exist_config.yaml");
  bool result_non_exist = reader_non_exist.JudgeAdapt();
  ASSERT_FALSE(result_non_exist);
}

TEST_F(ReaderTest, ReadSubgraphTest) {
  // Create a Reader object and set the work_dir_ to a known directory
  Reader reader("/home/baiwc/workspace/graph-systems/input/test/config.yaml");

  // initialize a Serialized object
  SerializedImmutableCSR* serialized_immutable_csr =
      new SerializedImmutableCSR();
  reader.SetPointer(serialized_immutable_csr);

  // Read a subgraph with enforce_adapt set to false
  reader.ReadSubgraph(0, false);
}

// TEST_F(ReaderTest, ReadSubgraphTest1) {
//   // Create a Reader object and set the work_dir_ to a known directory
//   Reader
//   reader("/Users/zhj/Projects/graph-systems/input/test_dir/config.yaml");

//   // Read a subgraph with enforce_adapt set to false
//   ASSERT_EXIT(reader.ReadSubgraph(1, false),
//   ::testing::ExitedWithCode(EXIT_FAILURE), ".*");
// }
}  // namespace sics::graph::core::io