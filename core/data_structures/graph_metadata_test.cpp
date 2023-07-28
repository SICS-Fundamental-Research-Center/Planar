//
// Created by Yang Liu on 2023/7/27.
//
#include "data_structures/graph_metadata.h"
#include "yaml-cpp/yaml.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace sics::graph::core::data_structures {

class GraphMetadataTest : public ::testing::Test {
 protected:
  GraphMetadataTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
};

TEST_F(GraphMetadataTest, NodeStructure) {
  YAML::Node metadata;
  metadata = YAML::LoadFile("../../testfile/meta.yaml");
  auto graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();
  EXPECT_EQ(graph_metadata.GetNumVertices(), 56);
  EXPECT_EQ(graph_metadata.GetNumEdges(), 100);
  EXPECT_EQ(graph_metadata.GetNumSubgraphs(), 2);
  std::vector<std::vector<int>> result_subgraph_metadata = {{28, 61, 100},
                                                            {28, 39, 101}};
  for (int i = 0; i < graph_metadata.GetNumSubgraphs(); i++) {
    auto subgraph_metadata = graph_metadata.GetSubgraphMetadata(i);
    EXPECT_EQ(subgraph_metadata.GetNumVertices(),
              result_subgraph_metadata[i][0]);
    EXPECT_EQ(subgraph_metadata.GetNumEdges(), result_subgraph_metadata[i][1]);
    EXPECT_EQ(subgraph_metadata.GetSize(), result_subgraph_metadata[i][2]);
  }
}

}  // namespace sics::graph::core::data_structures
