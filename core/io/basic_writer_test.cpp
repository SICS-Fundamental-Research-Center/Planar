#include <gtest/gtest.h>
#include <iostream>
#include <filesystem>

#include "io/basic_writer.h"
#include "io/basic_reader.h"
#include "data_structures/graph/immutable_csr_graph.h"
#include "util/logging.h"

namespace sics::graph::core::io {
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;

// The fixture for testing class LogTest
class WriterTest : public ::testing::Test {
 protected:
  WriterTest() = default;
  ~WriterTest() override = default;

  std::string data_dir = TEST_DATA_DIR;
  std::string subgraph_1_path = data_dir + "/input/small_graph_part/1";
  std::string write_1_path = data_dir + "/output/writer_test/1";
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
  reader.ReadSubgraph(subgraph_1_path, serialized_immutable_csr);
  LOG_INFO("end reading");

  // Read a subgraph
  EXPECT_NO_FATAL_FAILURE(writer.WriteSubgraph(write_1_path, serialized_immutable_csr));
  LOG_INFO("end writing");

  // Create another Reader object
  BasicReader rereader;

  // initialize a Serialized object
  SerializedImmutableCSRGraph* serialized_immutable_csr_1 =
      new SerializedImmutableCSRGraph();

  // Read a subgraph
  EXPECT_NO_FATAL_FAILURE(rereader.ReadSubgraph(write_1_path, serialized_immutable_csr_1));
}
}  // namespace sics::graph::core::io
