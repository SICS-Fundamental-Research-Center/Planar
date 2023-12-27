#include "miniclean/data_structures/graphs/miniclean_graph_metadata.h"

#include <gtest/gtest.h>

#include <fstream>

#include "core/util/logging.h"

namespace sics::graph::miniclean::data_structures::graphs {

class MiniCleanGraphMetadataTest : public ::testing::Test {
 protected:
  void SetUp() override {
    metadata_.num_vertices = 10;
    metadata_.num_edges = 20;
    metadata_.max_global_vid = 9;
    metadata_.min_global_vid = 0;
    metadata_.num_border_vertices = 3;
    metadata_.num_subgraphs = 1;
    metadata_.vlabel_id_to_local_vid_range = {{0, 9}, {10, 19}};
    metadata_.attr_id_to_attr_name = {"attr-0", "attr-1"};
    metadata_.attr_name_to_attr_id = {{"attr-0", 0}, {"attr-1", 1}};
    metadata_.label_id_to_label_name = {"label-0", "label-1"};
    metadata_.label_name_to_label_id = {{"label-0", 0}, {"label-1", 1}};

    subgraph_metadata_0_.gid = 0;
    subgraph_metadata_0_.num_vertices = 7;
    subgraph_metadata_0_.num_incoming_edges = 15;
    subgraph_metadata_0_.num_outgoing_edges = 10;
    subgraph_metadata_0_.max_global_vid = 6;
    subgraph_metadata_0_.min_global_vid = 0;
    subgraph_metadata_0_.vlabel_id_to_local_vid_range = {{0, 3}, {4, 7}};
    subgraph_metadata_0_.vattr_id_to_file_path = {"file/path-00",
                                                  "file/path-10"};
    subgraph_metadata_0_.vattr_id_to_vattr_type = {kUInt16, kString};
    subgraph_metadata_0_.vattr_id_to_max_string_length = {0, 20};
    subgraph_metadata_0_.vattr_id_to_vlabel_id = {0, 1};

    subgraph_metadata_1_.gid = 1;
    subgraph_metadata_1_.num_vertices = 6;
    subgraph_metadata_1_.num_incoming_edges = 8;
    subgraph_metadata_1_.num_outgoing_edges = 14;
    subgraph_metadata_1_.max_global_vid = 9;
    subgraph_metadata_1_.min_global_vid = 4;
    subgraph_metadata_1_.vlabel_id_to_local_vid_range = {{0, 2}, {3, 6}};
    subgraph_metadata_1_.vattr_id_to_file_path = {"file/path-01",
                                                  "file/path-11"};
    subgraph_metadata_1_.vattr_id_to_vattr_type = {kUInt64, kString};
    subgraph_metadata_1_.vattr_id_to_max_string_length = {0, 100};
    subgraph_metadata_1_.vattr_id_to_vlabel_id = {1, 1};

    metadata_.subgraphs = {subgraph_metadata_0_, subgraph_metadata_1_};
  }

  // Shared data structures for test cases.
  MiniCleanGraphMetadata metadata_;
  MiniCleanSubgraphMetadata subgraph_metadata_0_;
  MiniCleanSubgraphMetadata subgraph_metadata_1_;
  std::string data_dir_ = TEST_DATA_DIR;
};

TEST_F(MiniCleanGraphMetadataTest, TestDecode) {
  YAML::Node node = YAML::Node(metadata_);

  // Save the test sample to file. (Test encoding)
  std::ofstream fout(data_dir_ + "/output/miniclean_metadata_test/meta.yaml");
  YAML::Emitter emitter;

  emitter << node;
  fout << emitter.c_str();
  fout.close();

  // Load the test yaml file.
  YAML::Node metadata;
  EXPECT_NO_THROW(metadata = YAML::LoadFile(
                      data_dir_ + "/output/miniclean_metadata_test/meta.yaml"));

  // Test the decoding.
  MiniCleanGraphMetadata graph_metadata = metadata.as<MiniCleanGraphMetadata>();
  EXPECT_EQ(graph_metadata.num_vertices, metadata_.num_vertices);
  EXPECT_EQ(graph_metadata.num_edges, metadata_.num_edges);
  EXPECT_EQ(graph_metadata.max_global_vid, metadata_.max_global_vid);
  EXPECT_EQ(graph_metadata.min_global_vid, metadata_.min_global_vid);
  EXPECT_EQ(graph_metadata.num_border_vertices, metadata_.num_border_vertices);
  EXPECT_EQ(graph_metadata.num_subgraphs, metadata_.num_subgraphs);
  EXPECT_EQ(graph_metadata.vlabel_id_to_local_vid_range,
            metadata_.vlabel_id_to_local_vid_range);
  EXPECT_EQ(graph_metadata.attr_id_to_attr_name[0],
            metadata_.attr_id_to_attr_name[0]);
  EXPECT_EQ(graph_metadata.attr_id_to_attr_name[1],
            metadata_.attr_id_to_attr_name[1]);
  EXPECT_EQ(graph_metadata.attr_name_to_attr_id["attr-0"],
            metadata_.attr_name_to_attr_id["attr-0"]);
  EXPECT_EQ(graph_metadata.attr_name_to_attr_id["attr-1"],
            metadata_.attr_name_to_attr_id["attr-1"]);
  EXPECT_EQ(graph_metadata.label_id_to_label_name[0],
            metadata_.label_id_to_label_name[0]);
  EXPECT_EQ(graph_metadata.label_id_to_label_name[1],
            metadata_.label_id_to_label_name[1]);
  EXPECT_EQ(graph_metadata.label_name_to_label_id["label-0"],
            metadata_.label_name_to_label_id["label-0"]);
  EXPECT_EQ(graph_metadata.label_name_to_label_id["label-1"],
            metadata_.label_name_to_label_id["label-1"]);
  for (size_t i = 0; i < graph_metadata.num_subgraphs; i++) {
    EXPECT_EQ(graph_metadata.subgraphs[i].gid, metadata_.subgraphs[i].gid);
    EXPECT_EQ(graph_metadata.subgraphs[i].num_vertices,
              metadata_.subgraphs[i].num_vertices);
    EXPECT_EQ(graph_metadata.subgraphs[i].num_incoming_edges,
              metadata_.subgraphs[i].num_incoming_edges);
    EXPECT_EQ(graph_metadata.subgraphs[i].num_outgoing_edges,
              metadata_.subgraphs[i].num_outgoing_edges);
    EXPECT_EQ(graph_metadata.subgraphs[i].max_global_vid,
              metadata_.subgraphs[i].max_global_vid);
    EXPECT_EQ(graph_metadata.subgraphs[i].min_global_vid,
              metadata_.subgraphs[i].min_global_vid);
    EXPECT_EQ(graph_metadata.subgraphs[i].vlabel_id_to_local_vid_range.size(),
              metadata_.subgraphs[i].vlabel_id_to_local_vid_range.size());
    EXPECT_EQ(graph_metadata.subgraphs[i].vattr_id_to_file_path.size(),
              metadata_.subgraphs[i].vattr_id_to_file_path.size());
    for (size_t j = 0;
         j < graph_metadata.subgraphs[i].vlabel_id_to_local_vid_range.size();
         j++) {
      EXPECT_EQ(
          graph_metadata.subgraphs[i].vlabel_id_to_local_vid_range[j].first,
          metadata_.subgraphs[i].vlabel_id_to_local_vid_range[j].first);
      EXPECT_EQ(
          graph_metadata.subgraphs[i].vlabel_id_to_local_vid_range[j].second,
          metadata_.subgraphs[i].vlabel_id_to_local_vid_range[j].second);
    }
    for (size_t j = 0;
         j < graph_metadata.subgraphs[i].vattr_id_to_file_path.size(); j++) {
      EXPECT_EQ(graph_metadata.subgraphs[i].vattr_id_to_file_path[j],
                metadata_.subgraphs[i].vattr_id_to_file_path[j]);
    }
    EXPECT_EQ(graph_metadata.subgraphs[i].vattr_id_to_vattr_type.size(),
              metadata_.subgraphs[i].vattr_id_to_vattr_type.size());
    for (size_t j = 0;
         j < graph_metadata.subgraphs[i].vattr_id_to_vattr_type.size(); j++) {
      EXPECT_EQ(graph_metadata.subgraphs[i].vattr_id_to_vattr_type[j],
                metadata_.subgraphs[i].vattr_id_to_vattr_type[j]);
    }
  }
}

}  // namespace sics::graph::miniclean::data_structures::graphs