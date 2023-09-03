#include "data_structures/graph_metadata.h"

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <fstream>
#include <vector>

namespace sics::graph::core::data_structures {

class GraphMetadataTest : public ::testing::Test {
 protected:
  GraphMetadataTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }

  std::string data_dir = TEST_DATA_DIR;
  std::string meta_yaml_path = data_dir + "/meta.yaml";
  std::string meta_write_yaml_path = data_dir + "/meta_write.yaml";
};

// Tests reading the metadata from a YAML file.
TEST_F(GraphMetadataTest, NodeStructureRead) {
  YAML::Node metadata;
  try {
    metadata = YAML::LoadFile(meta_yaml_path);
  } catch (YAML::BadFile& e) {
    GTEST_LOG_(ERROR) << e.msg;
  }
  auto graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();
  EXPECT_EQ(graph_metadata.get_num_vertices(), 58);
  EXPECT_EQ(graph_metadata.get_num_edges(), 100);
  EXPECT_EQ(graph_metadata.get_max_vid(), 57);
  EXPECT_EQ(graph_metadata.get_min_vid(), 0);
  EXPECT_EQ(graph_metadata.get_num_subgraphs(), 2);
  std::vector<std::vector<int>> result_subgraph_metadata = {
      {0, 32, 87, 0, 47, 0}, {1, 26, 31, 0, 57, 5}};
  for (size_t i = 0; i < graph_metadata.get_num_subgraphs(); i++) {
    auto subgraph_metadata = graph_metadata.GetSubgraphMetadata(i);
    EXPECT_EQ(subgraph_metadata.gid, result_subgraph_metadata[i][0]);
    EXPECT_EQ(subgraph_metadata.num_vertices, result_subgraph_metadata[i][1]);
    EXPECT_EQ(subgraph_metadata.num_incoming_edges,
              result_subgraph_metadata[i][2]);
    EXPECT_EQ(subgraph_metadata.num_outgoing_edges,
              result_subgraph_metadata[i][3]);
    EXPECT_EQ(subgraph_metadata.max_vid, result_subgraph_metadata[i][4]);
    EXPECT_EQ(subgraph_metadata.min_vid, result_subgraph_metadata[i][5]);
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

  std::ofstream fout(meta_write_yaml_path);
  YAML::Node out;
  out["GraphMetadata"] = graph_metadata;
  auto res1 = out["subgraphs"];
  fout << out << std::endl;
}

}  // namespace sics::graph::core::data_structures
