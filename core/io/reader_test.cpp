#include "io/reader.h"
#include "data_structures/graph/serializable_immutable_csr.h"
#include "util/logging.h"
#include <gtest/gtest.h>
#include <iostream>

using SerializedImmutableCSR =
    sics::graph::core::data_structures::graph::SerializedImmutableCSR;

#define SUBGRAPH_PATH \
  "/home/baiwc/workspace/graph-systems/input/test/0/0_data.bin"

namespace sics::graph::core::io {

// The fixture for testing class LogTest
class ReaderTest : public ::testing::Test {
 protected:
  ReaderTest() = default;
  ~ReaderTest() override = default;
};

// Test the ReadSubgraph function of the Reader class
TEST_F(ReaderTest, ReadSubgraphTest) {
  // Create a Reader object
  Reader reader;

  // initialize a Serialized object
  SerializedImmutableCSR* serialized_immutable_csr =
      new SerializedImmutableCSR();

  // Read a subgraph
  reader.ReadSubgraph(SUBGRAPH_PATH, serialized_immutable_csr);

  LOG_INFO("end reading");
  uint8_t* a = serialized_immutable_csr->get_csr_buffer().front().front().Get(
      (4846609 - 5) * 4);
  uint32_t* a_uint32 = reinterpret_cast<uint32_t*>(a);
  for (std::size_t i = 0; i < 10; i++) {
    std::cout << "Element " << i << ": " << a_uint32[i] << std::endl;
  }
}

// Test the ReadSubgraph function of the Reader class
TEST_F(ReaderTest, ReadSubgraphTest1) {
  // Create a Reader object
  Reader reader;

  // initialize a Serialized object
  SerializedImmutableCSR* serialized_immutable_csr =
      new SerializedImmutableCSR();

  // Read a subgraph
  ASSERT_EXIT(reader.ReadSubgraph("non_exist_file", serialized_immutable_csr),
              ::testing::ExitedWithCode(EXIT_FAILURE), ".*");
}

// // Test the ReadBinFile function of the Reader class
// TEST_F(ReaderTest, ReadBinFileTest) {
//   // Create a Reader object and set the work_dir_ to a known directory
//   Reader reader();

//   // initialize a Serialized object
//   SerializedImmutableCSR* serialized_immutable_csr =
//       new SerializedImmutableCSR();
//   reader.SetPointer(serialized_immutable_csr);

//   // Read a subgraph with enforce_adapt set to false
//   reader.ReadBinFile(
//       "/Users/zhj/Projects/graph-systems/input/test_dir/0/0_data.bin");
// }
}  // namespace sics::graph::core::io
