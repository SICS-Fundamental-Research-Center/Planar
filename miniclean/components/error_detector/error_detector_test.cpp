#include "miniclean/components/error_detector/error_detector.h"

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <string>

#include "miniclean/common/error_detection_config.h"
#include "miniclean/components/error_detector/io_manager.h"
#include "miniclean/data_structures/graphs/miniclean_graph_metadata.h"

namespace sics::graph::miniclean::components::error_detector {

using ErrorDetectionConfig =
    sics::graph::miniclean::common::ErrorDetectionConfig;
using GraphMetadata =
    sics::graph::miniclean::data_structures::graphs::MiniCleanGraphMetadata;
using OpType = sics::graph::miniclean::data_structures::gcr::OpType;

class ErrorDetectorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    YAML::Node node;
    EXPECT_NO_THROW(
        node = YAML::LoadFile(
            data_dir +
            "/input/error_detector/graph/partition_result/meta.yaml"));
    graph_metadata_ = node.as<GraphMetadata>();
    ErrorDetectionConfig::Init(graph_metadata_);
  }
  std::string data_dir = TEST_DATA_DIR;
  GraphMetadata graph_metadata_;
};

TEST_F(ErrorDetectorTest, InitGCRSet) {
  std::string data_home = data_dir + "/input/error_detector/";
  std::string graph_home = data_dir + "/input/error_detector/graph/";
  IOManager io_manager(data_home, graph_home);
  ErrorDetector error_detector(&io_manager);
  error_detector.InitGCRSet();
  EXPECT_EQ(io_manager.GetGCRs().size(), 2);
  EXPECT_EQ(error_detector.GetAttributedPaths().size(), 3);

  auto paths = error_detector.GetAttributedPaths();

  auto path0 = error_detector.GetAttributedPaths()[0];
  EXPECT_EQ(path0.size(), 2);
  EXPECT_EQ(path0[0].label_id, 0);
  EXPECT_EQ(path0[0].attribute_ids.size(), 2);
  EXPECT_EQ(path0[0].attribute_ids[0], 3);
  EXPECT_EQ(path0[0].attribute_ids[1], 2);
  EXPECT_EQ(path0[0].attribute_values[0], "Comedy");
  EXPECT_EQ(path0[0].attribute_values[1], "2000");
  EXPECT_EQ(path0[0].op_types[0], OpType::kEq);
  EXPECT_EQ(path0[0].op_types[1], OpType::kGt);
  EXPECT_EQ(path0[1].label_id, 1);
  EXPECT_EQ(path0[1].attribute_ids.size(), 0);

  auto path1 = error_detector.GetAttributedPaths()[1];
  EXPECT_EQ(path1.size(), 2);
  EXPECT_EQ(path1[0].label_id, 0);
  EXPECT_EQ(path1[0].attribute_ids.size(), 1);
  EXPECT_EQ(path1[0].attribute_ids[0], 1);
  EXPECT_EQ(path1[0].attribute_values[0], "70");
  EXPECT_EQ(path1[0].op_types[0], OpType::kGt);
  EXPECT_EQ(path1[1].label_id, 2);
  EXPECT_EQ(path1[1].attribute_ids.size(), 1);
  EXPECT_EQ(path1[1].attribute_ids[0], 12);
  EXPECT_EQ(path1[1].attribute_values[0], "James Cameron");
  EXPECT_EQ(path1[1].op_types[0], OpType::kEq);

  auto path2 = error_detector.GetAttributedPaths()[2];
  EXPECT_EQ(path2.size(), 2);
  EXPECT_EQ(path2[0].label_id, 0);
  EXPECT_EQ(path2[0].attribute_ids.size(), 1);
  EXPECT_EQ(path2[0].attribute_ids[0], 1);
  EXPECT_EQ(path2[0].attribute_values[0], "70");
  EXPECT_EQ(path2[0].op_types[0], OpType::kGt);
  EXPECT_EQ(path2[1].label_id, 1);
  EXPECT_EQ(path2[1].attribute_ids.size(), 0);
}

TEST_F(ErrorDetectorTest, ComponentIO) {
  std::string data_home = data_dir + "/input/error_detector/";
  std::string graph_home = data_dir + "/input/error_detector/graph/";
  IOManager io_manager(data_home, graph_home);
  ErrorDetector error_detector(&io_manager);
  std::vector<ConstrainedStarInstance> partial_results;
  EXPECT_EQ(io_manager.GetSubgraphState(0), GraphStateType::kOnDisk);
  error_detector.LoadBasicComponents(0);
  EXPECT_EQ(io_manager.GetSubgraphState(0), GraphStateType::kInMemory);
  EXPECT_EQ(error_detector.GetGraph()->GetMetadata().gid, 0);
  EXPECT_EQ(error_detector.GetGraph()->GetMetadata().num_vertices, 6);
  error_detector.DischargePartialResults(partial_results);
  EXPECT_EQ(io_manager.GetSubgraphState(0), GraphStateType::kOnDisk);

  EXPECT_EQ(io_manager.GetSubgraphState(1), GraphStateType::kOnDisk);
  error_detector.LoadBasicComponents(1);
  EXPECT_EQ(io_manager.GetSubgraphState(1), GraphStateType::kInMemory);
  EXPECT_EQ(error_detector.GetGraph()->GetMetadata().gid, 1);
  EXPECT_EQ(error_detector.GetGraph()->GetMetadata().num_vertices, 6);
  error_detector.DischargePartialResults(partial_results);
  EXPECT_EQ(io_manager.GetSubgraphState(1), GraphStateType::kOnDisk);

  EXPECT_EQ(io_manager.GetSubgraphState(2), GraphStateType::kOnDisk);
  error_detector.LoadBasicComponents(2);
  EXPECT_EQ(io_manager.GetSubgraphState(2), GraphStateType::kInMemory);
  EXPECT_EQ(error_detector.GetGraph()->GetMetadata().gid, 2);
  EXPECT_EQ(error_detector.GetGraph()->GetMetadata().num_vertices, 3);
  error_detector.DischargePartialResults(partial_results);
  EXPECT_EQ(io_manager.GetSubgraphState(2), GraphStateType::kOnDisk);
}

}  // namespace sics::graph::miniclean::components::error_detector