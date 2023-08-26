#include "miniclean/io/miniclean_csr_reader.h"

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <memory>

#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/data_structures/graph_metadata.h"
#include "core/scheduler/message.h"
#include "core/util/logging.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

using GraphID = sics::graph::miniclean::common::GraphID;
using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using MiniCleanCSRGraph =
    sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
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

  // Test vertex 0.
  auto vertex_0 = graph.GetVertexByLocalID(0);
  // test vertex id
  EXPECT_EQ(vertex_0.vid, 0);
  // test degree
  EXPECT_EQ(vertex_0.indegree, 3);
  EXPECT_EQ(vertex_0.outdegree, 3);
  // test incoming edges
  auto incomeing_edges_0 = vertex_0.incoming_edges;
  EXPECT_EQ(incomeing_edges_0[0], 10);
  EXPECT_EQ(incomeing_edges_0[1], 23);
  EXPECT_EQ(incomeing_edges_0[2], 1);
  // test outgoing edges
  auto outgoing_edges_0 = vertex_0.outgoing_edges;
  EXPECT_EQ(outgoing_edges_0[0], 10);
  EXPECT_EQ(outgoing_edges_0[1], 23);
  EXPECT_EQ(outgoing_edges_0[2], 1);

  // Test vertex 30.
  auto vertex_30 = graph.GetVertexByLocalID(30);
  // test vertex id
  EXPECT_EQ(vertex_30.vid, 30);
  // test degree
  EXPECT_EQ(vertex_30.indegree, 1);
  EXPECT_EQ(vertex_30.outdegree, 3);
  // test incoming edges
  auto incomeing_edges_30 = vertex_30.incoming_edges;
  EXPECT_EQ(incomeing_edges_30[0], 24);
  // test outgoing edges
  auto outgoing_edges_30 = vertex_30.outgoing_edges;
  EXPECT_EQ(outgoing_edges_30[0], 24);
  EXPECT_EQ(outgoing_edges_30[1], 40);
  EXPECT_EQ(outgoing_edges_30[2], 43);

  // Test vertex 57.
  auto vertex_57 = graph.GetVertexByLocalID(57);
  // test vertex id
  EXPECT_EQ(vertex_57.vid, 57);
  // test degree
  EXPECT_EQ(vertex_57.indegree, 1);
  EXPECT_EQ(vertex_57.outdegree, 0);
  // test incoming edges
  auto incomeing_edges_57 = vertex_57.incoming_edges;
  EXPECT_EQ(incomeing_edges_57[0], 45);
}

}  // namespace sics::graph::miniclean::io
