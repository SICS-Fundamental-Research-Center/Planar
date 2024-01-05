#include "miniclean/io/miniclean_graph_reader.h"

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <cstring>
#include <memory>
#include <string>

#include "core/common/multithreading/thread_pool.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/graphs/miniclean_graph.h"
#include "miniclean/data_structures/graphs/serialized_miniclean_graph.h"
#include "miniclean/messages/message.h"

using SerializedMiniCleanGraph =
    sics::graph::miniclean::data_structures::graphs::SerializedMiniCleanGraph;
using ReadMessage = sics::graph::miniclean::messages::ReadMessage;
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
  uint32_t expected_movie_id[5] = {457056, 1285646, 2524904, 4925489, 5025257};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    const uint8_t* vptr = miniclean_graph.GetVertexAttributePtr(local_vid, 0);
    EXPECT_EQ(*((uint32_t*)vptr), expected_movie_id[local_vid]);
  }
  // attr. 1: movie_rating:uint8_t
  uint8_t expected_movie_rating[5] = {255, 88, 255, 59, 255};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    const uint8_t* vptr = miniclean_graph.GetVertexAttributePtr(local_vid, 1);
    EXPECT_EQ(*((uint8_t*)vptr), expected_movie_rating[local_vid]);
  }
  // attr. 2: movie_year:uint16_t
  uint16_t expected_movie_year[5] = {2004, 2007, 2009, 1996, 2011};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    const uint8_t* vptr = miniclean_graph.GetVertexAttributePtr(local_vid, 2);
    EXPECT_EQ(*((uint16_t*)vptr), expected_movie_year[local_vid]);
  }
  // attr. 3: movie_genre:string
  std::string expected_movie_genre[5] = {"Mixed", "Mixed", "Drama", "Mixed",
                                         "Thriller"};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    std::string str((char*)miniclean_graph.GetVertexAttributePtr(local_vid, 3));
    EXPECT_EQ(str, expected_movie_genre[local_vid]);
  }
  // attr. 4: movie_name:string
  std::string expected_movie_name[5] = {
      "Cosmetic Surgery Live (2004) {(#2.3)}", "Liar Game (2007) {(#2.4)}",
      "Unbounded Love Aka Ishq Ki Inteha (2009)", "Tsuki to kyabetsu (1996)",
      "Where Socks Go (2011)"};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    std::string str((char*)miniclean_graph.GetVertexAttributePtr(local_vid, 4));
    EXPECT_EQ(str, expected_movie_name[local_vid]);
  }
  // attr. 5: movie_title:string
  std::string expected_movie_title[5] = {"Cosmetic Surgery Live", "Liar Game",
                                         "Unbounded Love Aka Ishq Ki Inteha",
                                         "Tsuki to kyabetsu", "Where Socks Go"};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    std::string str((char*)miniclean_graph.GetVertexAttributePtr(local_vid, 5));
    EXPECT_EQ(str, expected_movie_title[local_vid]);
  }
  // attr. 6: movie_episode_name:string
  std::string expected_movie_episode_name[5] = {"", "", "", "", ""};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    std::string str((char*)miniclean_graph.GetVertexAttributePtr(local_vid, 6));
    EXPECT_EQ(str, expected_movie_episode_name[local_vid]);
  }
  // attr. 7: movie_episode_id:string
  std::string expected_movie_episode_id[5] = {"#2.3", "#2.4", "", "", ""};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    std::string str((char*)miniclean_graph.GetVertexAttributePtr(local_vid, 7));
    EXPECT_EQ(str, expected_movie_episode_id[local_vid]);
  }

  // attr. 8: movie_series_id:
  uint32_t expected_movie_series_id[5] = {188404, 553480, 1079129, 2144310,
                                          2197958};
  for (VertexID local_vid = 0; local_vid < miniclean_graph.GetNumVertices();
       local_vid++) {
    const uint8_t* vptr = miniclean_graph.GetVertexAttributePtr(local_vid, 8);
    EXPECT_EQ(*((uint32_t*)vptr), expected_movie_series_id[local_vid]);
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
  EXPECT_EQ(miniclean_graph.GetVertexLabel(3), 2);
  EXPECT_EQ(miniclean_graph.GetVertexLabel(4), 2);
  for (VertexID local_vid = 0; local_vid < 3; local_vid++) {
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
  // attr. 9: cast_vid:uint32_t
  uint32_t expected_cast_id[3] = {3796340, 4008929, 5077778};
  for (VertexID local_vid = 0; local_vid < 3; local_vid++) {
    const uint8_t* vptr = miniclean_graph.GetVertexAttributePtr(local_vid, 9);
    EXPECT_EQ(*((uint32_t*)vptr), expected_cast_id[local_vid]);
  }
  // attr. 10: cast_name:string
  std::string expected_cast_name[3] = {"Jutta_Yûki", "M._Nawaz",
                                       "Yûkei_Hasegawa"};
  for (VertexID local_vid = 0; local_vid < 3; local_vid++) {
    std::string str(
        (char*)miniclean_graph.GetVertexAttributePtr(local_vid, 10));
    EXPECT_EQ(str, expected_cast_name[local_vid]);
  }

  // attr. 11: director_vid:uint32_t
  uint32_t expected_director_id[2] = {3341147, 3722583};
  for (VertexID local_vid = 3; local_vid < 5; local_vid++) {
    const uint8_t* vptr = miniclean_graph.GetVertexAttributePtr(local_vid, 11);
    EXPECT_EQ(*((uint32_t*)vptr), expected_director_id[local_vid - 3]);
  }
  // attr. 12: director_name:string
  std::string expected_director_name[2] = {"Elliot_Kew", "Jesse (I)_Dillon"};
  for (VertexID local_vid = 3; local_vid < 5; local_vid++) {
    std::string str(
        (char*)miniclean_graph.GetVertexAttributePtr(local_vid, 12));
    EXPECT_EQ(str, expected_director_name[local_vid - 3]);
  }
}
}  // namespace sics::graph::miniclean::io
