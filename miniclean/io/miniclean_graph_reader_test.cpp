#include "miniclean/io/miniclean_graph_reader.h"

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <memory>

// using SerializedMiniCleanGraph =
//     sics::graph::miniclean::data_structures::graphs::SerializedMiniCleanGraph;

namespace sics::graph::miniclean::io {

class MiniCleanGraphReaderTest : public ::testing::Test {
 protected:
  MiniCleanGraphReaderTest() = default;
  ~MiniCleanGraphReaderTest() override = default;

  std::string data_dir_ = TEST_DATA_DIR;
  std::string graph_home = data_dir_ + "/input/miniclean_graph_reader/";
};

TEST_F(MiniCleanGraphReaderTest, ReadSubgraphTest) {
  // Create a Reader.
  MiniCleanGraphReader reader(graph_home);

  // Load metadata.
  YAML::Node graph_metadata;
  EXPECT_NO_THROW(graph_metadata = YAML::LoadFile(
                      graph_home + "partition_result/meta.yaml"));

  // Initialize a `Serialized` object.
  // std::unique_ptr<SerializedMiniCleanGraph> serialized_graph_;
}
}  // namespace sics::graph::miniclean::io
