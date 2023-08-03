#include "data_structures/graph_metadata.h"
#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>
#include <fstream>

#define SOURCE_DIR "${CMAKE_SOURCE_DIR}"

namespace sics::graph::core::data_structures {

class GraphMetadataTest : public ::testing::Test {
 protected:
  GraphMetadataTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }

  std::string data_dir = TEST_DATA_DIR;
  std::string meta_yaml_path = data_dir + "/meta.yaml";
};

TEST_F(GraphMetadataTest, NodeStructureRead) {
  YAML::Node metadata;
  try {
    metadata = YAML::LoadFile(meta_yaml_path);
  } catch (YAML::BadFile& e) {
    GTEST_LOG_(ERROR) << e.msg;
  }
  auto graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();
  EXPECT_EQ(graph_metadata.get_num_vertices(), 56);
  EXPECT_EQ(graph_metadata.get_num_edges(), 100);
  EXPECT_EQ(graph_metadata.get_num_subgraphs(), 2);
  std::vector<std::vector<int>> result_subgraph_metadata = {{28, 61, 100},
                                                            {28, 39, 101}};
  for (int i = 0; i < graph_metadata.get_num_subgraphs(); i++) {
    auto subgraph_metadata = graph_metadata.GetSubgraphMetadata(i);
    EXPECT_EQ(subgraph_metadata.num_vertices_, result_subgraph_metadata[i][0]);
    EXPECT_EQ(subgraph_metadata.num_edges_, result_subgraph_metadata[i][1]);
    EXPECT_EQ(subgraph_metadata.size_, result_subgraph_metadata[i][2]);
  }
}

TEST_F(GraphMetadataTest, NodeStructureWrite) {
  YAML::Node metadata;
  try {
    metadata = YAML::LoadFile(meta_yaml_path);
  } catch (YAML::BadFile& e) {
    GTEST_LOG_(ERROR) << e.msg;
  }
  auto graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();

  std::ofstream fout("../../testfile/meta_write.yaml");
  YAML::Node out;
  out["GraphMetadata"] = graph_metadata;
  auto res1 = out["subgraphs"];
  fout << out << std::endl;
}

}  // namespace sics::graph::core::data_structures
