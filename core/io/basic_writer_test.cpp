#include <gtest/gtest.h>
#include <iostream>
#include <filesystem>

#include "io/basic_writer.h"
#include "io/basic_reader.h"
#include "data_structures/graph/immutable_csr_graph.h"
#include "util/logging.h"

#define SUBGRAPH_0_PATH \
  "../../../input/small_graph_part/0"
#define SUBGRAPH_1_PATH \
  "../../../input/small_graph_part/1"
#define WRITE_0_PATH \
  "../../../output/writer_test/0"
#define WRITE_1_PATH \
  "../../../output/writer_test/1"

namespace sics::graph::core::io {
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;

// The fixture for testing class LogTest
class WriterTest : public ::testing::Test {
 protected:
  WriterTest() = default;
  ~WriterTest() override = default;
};

// Test the WriteSubgraph function of the Writer class
TEST_F(WriterTest, WriteSubgraph) {
  // Create a Reader object
  BasicReader reader;
  // Create a Writer object
  BasicWriter writer;

  // initialize a Serialized object
  SerializedImmutableCSRGraph* serialized_immutable_csr =
      new SerializedImmutableCSRGraph();

  // Read a subgraph
  reader.ReadSubgraph(SUBGRAPH_1_PATH, serialized_immutable_csr);
  LOG_INFO("end reading");

  // Read a subgraph
  writer.WriteSubgraph(WRITE_1_PATH, serialized_immutable_csr);
  LOG_INFO("end writing");

  // Create another Reader object
  BasicReader rereader;

  // initialize a Serialized object
  SerializedImmutableCSRGraph* serialized_immutable_csr_1 =
      new SerializedImmutableCSRGraph();

  // Read a subgraph
  rereader.ReadSubgraph(WRITE_1_PATH, serialized_immutable_csr_1);

  uint8_t* data = serialized_immutable_csr_1->GetCSRBuffer().front().front().Get();
  // size_t size = serialized_immutable_csr->get_csr_buffer().front().front().GetSize();
  uint32_t* a_uint32 = reinterpret_cast<uint32_t*>(data);
  size_t* a_size_t = reinterpret_cast<size_t*>(data);
  // for (std::size_t i = 0; i < 28; i++) {
  //     std::cout << "Element " << i << ": " << a_uint32[i] << std::endl;
  // }
  // std::cout << "##########" << std::endl;
  // for (std::size_t i = 14; i < 14+28*4; i++) {
  //     std::cout << "Element " << i+14 << ": " << a_size_t[i] << std::endl;
  // }
  // std::cout << "##########" << std::endl;
  // for (std::size_t i = (14+28*4)*2; i < (14+28*4)*2+71*2; i++) {
  //     std::cout << "Element " << i-4*28 << ": " << a_uint32[i] << std::endl;
  // }
  // std::cout << "##########" << std::endl;
}

// Test the WriteSubgraph function of the Writer class
TEST_F(WriterTest, WriteSubgraph1) {
  // Create a Reader object
  BasicReader reader;
  // Create a Writer object
  BasicWriter writer;

  // initialize a Serialized object
  SerializedImmutableCSRGraph* serialized_immutable_csr =
      new SerializedImmutableCSRGraph();

  // Read a subgraph
  reader.ReadSubgraph(SUBGRAPH_1_PATH, serialized_immutable_csr);

  // Read a subgraph
  writer.WriteSubgraph(WRITE_1_PATH, serialized_immutable_csr);
}
}  // namespace sics::graph::core::io
