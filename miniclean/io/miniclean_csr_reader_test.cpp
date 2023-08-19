#include "miniclean/io/miniclean_csr_reader.h"

#include <memory>

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include "core/common/types.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/data_structures/graph_metadata.h"
#include "core/util/logging.h"
#include "core/scheduler/message.h"
#include "miniclean/graphs/miniclean_csr_graph.h"

using GraphID = sics::graph::core::common::GraphID;
using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using MiniCleanCSRGraph = sics::graph::miniclean::graphs::MiniCleanCSRGraph;
using ReadMessage = sics::graph::core::scheduler::ReadMessage;
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using ThreadPool = sics::graph::core::common::ThreadPool;

namespace sics::graph::miniclean::io {

// The fixture for testing class LogTest
class MiniCleanCSRReaderTest : public ::testing::Test {
 protected:
  MiniCleanCSRReaderTest() = default;
  ~MiniCleanCSRReaderTest() override = default;

  std::string data_dir_ = TEST_DATA_DIR;
  std::string subgraph_path_ = data_dir_ + "/input/small_graph_path_matching";
  GraphID gid_ = 0;
};

// Test the ReadSubgraph function of the Reader class
TEST_F(MiniCleanCSRReaderTest, ReadSubgraphTest) {
  // Create a Reader object.
  MiniCleanCSRReader reader(subgraph_path_);

  // Load metadata.
  YAML::Node metadata;
  try {
    metadata = YAML::LoadFile(subgraph_path_ + "/meta.yaml");
  } catch (YAML::BadFile& e) {
    GTEST_LOG_(ERROR) << e.msg;
  }
  GraphMetadata graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();

  // initialize a Serialized object.
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_immutable_csr =
      std::make_unique<SerializedImmutableCSRGraph>();

  // Initialize a ReadMessage object.
  ReadMessage read_message;
  read_message.graph_id = gid_;
  read_message.response_serialized = serialized_immutable_csr.get();

  // Read a subgraph
  reader.Read(&read_message, nullptr);

  // Initialize MiniCleanCSRGraph object.
  MiniCleanCSRGraph graph(graph_metadata.GetSubgraphMetadata(gid_));

  // Deserializing.
  ThreadPool thread_pool(1);
  graph.Deserialize(thread_pool, std::move(serialized_immutable_csr));

  graph.ShowGraph(100);

  /* Test vertex id, degree, related edges, labels, and attributes */
  // TODO (bai-wenchao): Add tests for labels and attributes.
  // TODO (bai-wenchao): Add tests for incoming edges and outgoing edges, since
  //                     the order of edge list is inconsistent with that
  //                     provided by Xiaoke. Although the content of two
  //                     edgelists are the same, it needs a further check.

  // Test vertex 0.
  auto vertex_0 = graph.GetVertexByLocalID(0);
  EXPECT_EQ(vertex_0.vid, 0);
  EXPECT_EQ(vertex_0.indegree, 3);
  EXPECT_EQ(vertex_0.outdegree, 3);

  // Test vertex 30.
  auto vertex_30 = graph.GetVertexByLocalID(30);
  EXPECT_EQ(vertex_30.vid, 30);
  EXPECT_EQ(vertex_30.indegree, 1);
  EXPECT_EQ(vertex_30.outdegree, 3);

  // Test vertex 57.
  auto vertex_57 = graph.GetVertexByLocalID(57);
  EXPECT_EQ(vertex_57.vid, 57);
  EXPECT_EQ(vertex_57.indegree, 1);
  EXPECT_EQ(vertex_57.outdegree, 0);
}

}  // namespace sics::graph::miniclean::io
