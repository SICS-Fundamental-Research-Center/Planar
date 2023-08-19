#include <gtest/gtest.h>
#include <iostream>

#include "io/basic_reader.h"
#include "data_structures/graph/immutable_csr_graph.h"
#include "util/logging.h"

using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;

namespace sics::graph::core::io {

// The fixture for testing class LogTest
class ReaderTest : public ::testing::Test {
 protected:
  ReaderTest() = default;
  ~ReaderTest() override = default;

  std::string data_dir = TEST_DATA_DIR;
  std::string subgraph_1_path = data_dir + "/input/small_graph_part/1";
};

// Test the ReadSubgraph function of the Reader class
TEST_F(ReaderTest, ReadSubgraphTest) {
  // Create a Reader object
  BasicReader reader;

  // initialize a Serialized object
  SerializedImmutableCSRGraph* serialized_immutable_csr =
      new SerializedImmutableCSRGraph();

  // Read a subgraph
  reader.ReadSubgraph(subgraph_1_path, serialized_immutable_csr);

  LOG_INFO("end reading");
  uint8_t* data = serialized_immutable_csr->GetCSRBuffer().front().front().Get();
  // size_t size = serialized_immutable_csr->get_csr_buffer().front().front().GetSize();
  uint32_t* a_uint32 = reinterpret_cast<uint32_t*>(data);
  size_t* a_size_t = reinterpret_cast<size_t*>(data);
  for (std::size_t i = 0; i < 28; i++) {
      std::cout << "Element " << i << ": " << a_uint32[i] << std::endl;
  }
  std::cout << "##########" << std::endl;
  for (std::size_t i = 14; i < 14+28*4; i++) {
      std::cout << "Element " << i+14 << ": " << a_size_t[i] << std::endl;
  }
  std::cout << "##########" << std::endl;
  for (std::size_t i = (14+28*4)*2; i < (14+28*4)*2+71*2; i++) {
      std::cout << "Element " << i-4*28 << ": " << a_uint32[i] << std::endl;
  }
  std::cout << "##########" << std::endl;
}

// Test the ReadSubgraph function of the Reader class
TEST_F(ReaderTest, ReadSubgraphReadTypeTest) {
  // Create a Reader object
  BasicReader reader;

  // initialize a Serialized object
  SerializedImmutableCSRGraph* serialized_immutable_csr =
      new SerializedImmutableCSRGraph();

  // Read a subgraph
  reader.ReadSubgraph(subgraph_1_path, serialized_immutable_csr, 1);
}

// Test the ReadSubgraph function of the Reader class
TEST_F(ReaderTest, ReadSubgraphTest1) {
  // Create a Reader object
  BasicReader reader;

  // initialize a Serialized object
  SerializedImmutableCSRGraph* serialized_immutable_csr =
      new SerializedImmutableCSRGraph();

  // Read a subgraph
  ASSERT_EXIT(reader.ReadSubgraph("non_exist_file", serialized_immutable_csr),
              ::testing::ExitedWithCode(EXIT_FAILURE), ".*");
}
}  // namespace sics::graph::core::io
