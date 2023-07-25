#include "io/reader.h"
#include "data_structures/graph/serializable_immutable_csr.h"
#include <gtest/gtest.h>
#include <iostream>

using SerializedImmutableCSR =
    sics::graph::core::data_structures::graph::SerializedImmutableCSR;

#define CONFIG_PATH \
  "/Users/zhj/Projects/graph-systems/input/test_dir/config.yaml"

namespace sics::graph::core::io {

// The fixture for testing class LogTest
class ReaderTest : public ::testing::Test {
 protected:
  ReaderTest() = default;
  ~ReaderTest() override = default;
};

// Test the JudgeAdapt function of the Reader class
TEST_F(ReaderTest, JudgeAdaptTest) {
  // Test with an existing config file
  Reader reader(CONFIG_PATH);
  bool result = reader.JudgeAdapt();
  ASSERT_TRUE(result);
}

// Test the init function of the Reader class
TEST_F(ReaderTest, ReaderConstructionTest) {
  // Test with an non existing config file
  ASSERT_EXIT(Reader reader("non_existing_config.yaml"),
              ::testing::ExitedWithCode(EXIT_FAILURE), ".*");
}

// Test the ReadSubgraph function of the Reader class
TEST_F(ReaderTest, ReadSubgraphTest) {
  // Create a Reader object and set the work_dir_ to a known directory
  Reader reader(CONFIG_PATH);

  // initialize a Serialized object
  SerializedImmutableCSR* serialized_immutable_csr =
      new SerializedImmutableCSR();
  reader.SetPointer(serialized_immutable_csr);

  // Read a subgraph with enforce_adapt set to false
  reader.ReadSubgraph(0, false);
}

// Test the ReadSubgraph function of the Reader class
TEST_F(ReaderTest, ReadSubgraphTest1) {
  // Create a Reader object and set the work_dir_ to a known directory
  Reader reader(CONFIG_PATH);

  // Read a subgraph with enforce_adapt set to false
  ASSERT_EXIT(reader.ReadSubgraph(1, false),
              ::testing::ExitedWithCode(EXIT_FAILURE), ".*");
}

// Test the ReadBinFile function of the Reader class
TEST_F(ReaderTest, ReadBinFileTest) {
  // Create a Reader object and set the work_dir_ to a known directory
  Reader reader(CONFIG_PATH);

  // initialize a Serialized object
  SerializedImmutableCSR* serialized_immutable_csr =
      new SerializedImmutableCSR();
  reader.SetPointer(serialized_immutable_csr);

  // Read a subgraph with enforce_adapt set to false
  reader.ReadBinFile(
      "/Users/zhj/Projects/graph-systems/input/test_dir/0/0_data.bin");
}
}  // namespace sics::graph::core::io
