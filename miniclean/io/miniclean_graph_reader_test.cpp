#include "miniclean/io/miniclean_graph_reader.h"

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <cstring>
#include <memory>
#include <string>

#include "core/common/multithreading/thread_pool.h"
#include "core/scheduler/message.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/graphs/miniclean_graph.h"
#include "miniclean/data_structures/graphs/serialized_miniclean_graph.h"

using SerializedMiniCleanGraph =
    sics::graph::miniclean::data_structures::graphs::SerializedMiniCleanGraph;
using ReadMessage = sics::graph::core::scheduler::ReadMessage;
using MiniCleanGraph =
    sics::graph::miniclean::data_structures::graphs::MiniCleanGraph;
using MiniCleanGraphMetadata =
    sics::graph::miniclean::data_structures::graphs::MiniCleanGraphMetadata;
using MiniCleanSubgraphMetadata =
    sics::graph::miniclean::data_structures::graphs::MiniCleanSubgraphMetadata;
using ThreadPool = sics::graph::core::common::ThreadPool;
using VertexID = sics::graph::miniclean::common::VertexID;

namespace sics::graph::miniclean::io {

class MiniCleanGraphReaderTest : public ::testing::Test {
 protected:
  MiniCleanGraphReaderTest() = default;
  ~MiniCleanGraphReaderTest() override = default;

  std::string data_dir_ = TEST_DATA_DIR;
  std::string graph_home = data_dir_ + "/input/miniclean_graph_reader/";
};

TEST_F(MiniCleanGraphReaderTest, ReadSubgraph0Test) {
  // Create a Reader.
  MiniCleanGraphReader reader(graph_home);

  // Load metadata.
  YAML::Node node;
  EXPECT_NO_THROW(
      node = YAML::LoadFile(graph_home + "partition_result/meta.yaml"));
  MiniCleanGraphMetadata graph_metadata = node.as<MiniCleanGraphMetadata>();

  // Initialize a `Serialized` object.
  std::unique_ptr<SerializedMiniCleanGraph> serialized_graph_ =
      std::make_unique<SerializedMiniCleanGraph>();

  // Initialize a `ReadMessage` object.
  ReadMessage read_message;
  read_message.graph_id = 0;
  read_message.response_serialized = serialized_graph_.get();

  // Read subgraph 0.
  reader.Read(&read_message, nullptr);

  // Initialize MiniCleanGraph object for subgraph 0.
  MiniCleanGraph miniclean_graph(graph_metadata.subgraphs[0],
                                 graph_metadata.num_vertices);

  // Deserializing.
  ThreadPool thread_pool(1);
  miniclean_graph.Deserialize(thread_pool, std::move(serialized_graph_));

  // Check vertex label.
  EXPECT_EQ(miniclean_graph.GetNumVertices(), 5);
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    EXPECT_EQ(miniclean_graph.GetVertexLabel(local_vid), 0);
  }

  // Check bitmap.
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    VertexID global_vid = miniclean_graph.GetVertexGlobalID(local_vid);
    EXPECT_TRUE(miniclean_graph.IsInGraph(global_vid));
  }
  for (VertexID global_vid = 5; global_vid < 10; global_vid++) {
    EXPECT_FALSE(miniclean_graph.IsInGraph(global_vid));
  }

  // Check vertex attribute.
  // attr. 0: movie_vid:uint32_t
  uint32_t expected_movie_id[5] = {635340, 3507988, 4643261, 4853399, 5013154};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    const uint8_t* vptr = miniclean_graph.GetVertexAttributePtr(local_vid, 0);
    EXPECT_EQ(*((uint32_t*)vptr), expected_movie_id[local_vid]);
  }
  // attr. 1: movie_rating:uint8_t
  uint8_t expected_movie_rating[5] = {79, 255, 255, 255, 255};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    const uint8_t* vptr = miniclean_graph.GetVertexAttributePtr(local_vid, 1);
    EXPECT_EQ(*((uint8_t*)vptr), expected_movie_rating[local_vid]);
  }
  // attr. 2: movie_year:uint16_t
  uint16_t expected_movie_year[5] = {1994, 2012, 2015, 1992, 1999};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    const uint8_t* vptr = miniclean_graph.GetVertexAttributePtr(local_vid, 2);
    EXPECT_EQ(*((uint16_t*)vptr), expected_movie_year[local_vid]);
  }
  // attr. 3: movie_genre:string
  std::string expected_movie_genre[5] = {"Mixed", "Adult", "Adult", "Adult",
                                         "Adult"};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    std::string str((char*)miniclean_graph.GetVertexAttributePtr(local_vid, 3));
    EXPECT_EQ(str, expected_movie_genre[local_vid]);
  }
  // attr. 4: movie_name:string
  std::string expected_movie_name[5] = {
      "Duckman: Private Dick/Family Man (1994) {Role with It (#4.7)}",
      "Gorgeous Rodeo Girl (2012) (V)", "Spring Break Orgy (2015) (V)",
      "The Uncut Version (1992) (V)", "We Go Deep 2 (1999) (V)"};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    std::string str((char*)miniclean_graph.GetVertexAttributePtr(local_vid, 4));
    EXPECT_EQ(str, expected_movie_name[local_vid]);
  }
  // attr. 5: movie_title:string
  std::string expected_movie_title[5] = {
      "Duckman: Private Dick/Family Man", "Gorgeous Rodeo Girl",
      "Spring Break Orgy", "The Uncut Version", "We Go Deep 2"};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    std::string str((char*)miniclean_graph.GetVertexAttributePtr(local_vid, 5));
    EXPECT_EQ(str, expected_movie_title[local_vid]);
  }
  // attr. 6: movie_episode_name:string
  std::string expected_movie_episode_name[5] = {"Role with It", "", "", "", ""};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    std::string str((char*)miniclean_graph.GetVertexAttributePtr(local_vid, 6));
    EXPECT_EQ(str, expected_movie_episode_name[local_vid]);
  }
  // attr. 7: movie_episode_id:string
  std::string expected_movie_episode_id[5] = {"#4.7", "", "", "", ""};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    std::string str((char*)miniclean_graph.GetVertexAttributePtr(local_vid, 7));
    EXPECT_EQ(str, expected_movie_episode_id[local_vid]);
  }
}

TEST_F(MiniCleanGraphReaderTest, ReadSubgraph1Test) {
  // Create a Reader.
  MiniCleanGraphReader reader(graph_home);

  // Load metadata.
  YAML::Node node;
  EXPECT_NO_THROW(
      node = YAML::LoadFile(graph_home + "partition_result/meta.yaml"));
  MiniCleanGraphMetadata graph_metadata = node.as<MiniCleanGraphMetadata>();

  // Initialize a `Serialized` object.
  std::unique_ptr<SerializedMiniCleanGraph> serialized_graph_ =
      std::make_unique<SerializedMiniCleanGraph>();

  // Initialize a `ReadMessage` object.
  ReadMessage read_message;
  read_message.graph_id = 1;
  read_message.response_serialized = serialized_graph_.get();

  // Read subgraph 1.
  reader.Read(&read_message, nullptr);

  // Initialize MiniCleanGraph object for subgraph 1.
  MiniCleanGraph miniclean_graph(graph_metadata.subgraphs[1],
                                 graph_metadata.num_vertices);

  // Deserializing.
  ThreadPool thread_pool(1);
  miniclean_graph.Deserialize(thread_pool, std::move(serialized_graph_));

  // Check vertex label.
  EXPECT_EQ(miniclean_graph.GetNumVertices(), 5);
  EXPECT_EQ(miniclean_graph.GetVertexLabel(4), 2);
  for (VertexID local_vid = 0; local_vid < 4; local_vid++) {
    EXPECT_EQ(miniclean_graph.GetVertexLabel(local_vid), 1);
  }

  // Check bitmap.
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    VertexID global_vid = miniclean_graph.GetVertexGlobalID(local_vid);
    EXPECT_TRUE(miniclean_graph.IsInGraph(global_vid));
  }
  for (VertexID global_vid = 0; global_vid < 5; global_vid++) {
    EXPECT_FALSE(miniclean_graph.IsInGraph(global_vid));
  }

  // Check vertex attribute.
  // attr. 8: cast_vid:uint32_t
  uint32_t expected_cast_id[4] = {2985507, 3021966, 3583720, 4904266};
  for (VertexID local_vid = 0; local_vid < 4; local_vid++) {
    const uint8_t* vptr = miniclean_graph.GetVertexAttributePtr(local_vid, 8);
    EXPECT_EQ(*((uint32_t*)vptr), expected_cast_id[local_vid]);
  }
  // attr. 9: cast_name:string
  std::string expected_cast_name[4] = {"Billy_Glide", "Brett (IV)_",
                                       "Hitomi_Kitagawa", "Tony_Everready"};
  for (VertexID local_vid = 0; local_vid < 4; local_vid++) {
    std::string str((char*)miniclean_graph.GetVertexAttributePtr(local_vid, 9));
    EXPECT_EQ(str, expected_cast_name[local_vid]);
  }

  // attr. 10: director_vid:uint32_t
  uint32_t expected_director_id = 2884075;
  const uint8_t* vptr = miniclean_graph.GetVertexAttributePtr(4, 10);
  EXPECT_EQ(*((uint32_t*)vptr), expected_director_id);
  // attr. 11: director_name:string
  std::string expected_director_name = "Anthony (I)_Bell";
  std::string str((char*)miniclean_graph.GetVertexAttributePtr(4, 11));
  EXPECT_EQ(str, expected_director_name);
}
}  // namespace sics::graph::miniclean::io
