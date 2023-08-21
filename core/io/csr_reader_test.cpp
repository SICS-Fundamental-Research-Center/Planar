#include <iostream>

#include <gtest/gtest.h>

#include "io/csr_reader.h"
#include "common/multithreading/thread_pool.h"
#include "data_structures/graph/immutable_csr_graph.h"
#include "util/logging.h"

namespace sics::graph::core::io {

using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using ReadMessage = sics::graph::core::scheduler::ReadMessage;
using ThreadPool = sics::graph::core::common::ThreadPool;

// The fixture for testing class LogTest
class CSRReaderTest : public ::testing::Test {
 protected:
  CSRReaderTest() = default;
  ~CSRReaderTest() override = default;

  const std::string root_path_ =
      std::string(TEST_DATA_DIR) + "/edgecut_csr_in/";
};

// Test the ReadSubgraph function of the Reader class
TEST_F(CSRReaderTest, ReadShouldNotHaveFatalFalure) {
  auto metadata = YAML::LoadFile(root_path_ + "meta.yaml");
  auto graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();

  ThreadPool thread_pool(1);

  CSRReader csr_reader(root_path_);
  ReadMessage read_message_0, read_message_1;
  auto serialized_csr_0 = std::make_unique<SerializedImmutableCSRGraph>();
  auto serialized_csr_1 = std::make_unique<SerializedImmutableCSRGraph>();

  read_message_0.graph_id = 0;
  read_message_0.response_serialized = serialized_csr_0.get();
  EXPECT_NO_FATAL_FAILURE(csr_reader.Read(&read_message_0, nullptr));

  read_message_1.graph_id = 1;
  read_message_1.response_serialized = serialized_csr_1.get();
  EXPECT_NO_FATAL_FAILURE(csr_reader.Read(&read_message_1, nullptr));
}

}  // namespace sics::graph::core::io