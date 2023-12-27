#include "tools/graph_partitioner/partitioner/bfs_based_edgecut.h"

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <memory>

#include "core/common/multithreading/task.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/common/types.h"
#include "core/data_structures/graph/immutable_csr_graph.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/data_structures/graph_metadata.h"
#include "core/io/csr_reader.h"
#include "core/scheduler/message.h"

namespace sics::graph::tools::graph_partitioner {

using CSRReader = sics::graph::core::io::CSRReader;
using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using ThreadPool = sics::graph::core::common::ThreadPool;
using ReadMessage = sics::graph::core::scheduler::ReadMessage;
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using ImmutableCSRGraph =
    sics::graph::core::data_structures::graph::ImmutableCSRGraph;
using VertexID = sics::graph::core::common::VertexID;

class PartitionerTest : public ::testing::Test {
 protected:
  PartitionerTest() {
    YAML::Node node = YAML::LoadFile(part_home_ + "meta.yaml");
    metadata_ = node["GraphMetadata"].as<GraphMetadata>();
  };

  ~PartitionerTest() override = default;

  std::string data_dir_ = TEST_DATA_DIR;
  std::string part_home_ = data_dir_ + "/input/bfs_edgecut/partition_result/";
  GraphMetadata metadata_;
};

TEST_F(PartitionerTest, SubgraphTest) {
  // Test subgraph 0
  CSRReader reader(part_home_);
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_graph_0 =
      std::make_unique<SerializedImmutableCSRGraph>();
  ReadMessage msg_0;
  msg_0.graph_id = 0;
  msg_0.response_serialized = serialized_graph_0.get();
  reader.Read(&msg_0, nullptr);

  ImmutableCSRGraph graph_0(metadata_.GetSubgraphMetadata(0));
  ThreadPool thread_pool(1);
  graph_0.Deserialize(thread_pool, std::move(serialized_graph_0));
  EXPECT_EQ(graph_0.get_num_vertices(), 4);
  for (VertexID vid = 0; vid < graph_0.get_num_vertices(); vid++) {
    auto u = graph_0.GetVertexByLocalID(vid);
    if (u.vid == 2) {
      EXPECT_EQ(u.outdegree, 4);
      EXPECT_EQ(u.indegree, 4);
    } else {
      EXPECT_EQ(u.outdegree, 3);
      EXPECT_EQ(u.indegree, 3);
    }
  }

  // Test subgraph 1
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_graph_1 =
      std::make_unique<SerializedImmutableCSRGraph>();
  ReadMessage msg_1;
  msg_1.graph_id = 1;
  msg_1.response_serialized = serialized_graph_1.get();
  reader.Read(&msg_1, nullptr);

  ImmutableCSRGraph graph_1(metadata_.GetSubgraphMetadata(1));
  graph_1.Deserialize(thread_pool, std::move(serialized_graph_1));

  EXPECT_EQ(graph_1.get_num_vertices(), 4);
  for (VertexID vid = 0; vid < graph_1.get_num_vertices(); vid++) {
    auto u = graph_1.GetVertexByLocalID(vid);
    if (u.vid == 7) {
      EXPECT_EQ(u.outdegree, 4);
      EXPECT_EQ(u.indegree, 4);
    } else {
      EXPECT_EQ(u.outdegree, 3);
      EXPECT_EQ(u.indegree, 3);
    }
  }

}  // namespace sics::graph::tools::graph_partitioner
}  // namespace sics::graph::tools::graph_partitioner