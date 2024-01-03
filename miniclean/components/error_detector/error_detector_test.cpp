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
    io_manager_ = IOManager(data_dir + "/input/error_detector/",
                           data_dir + "/input/error_detector/graph/");
    error_detector_ = ErrorDetector(&io_manager_);
  }
  std::string data_dir = TEST_DATA_DIR;
  IOManager io_manager_;
  ErrorDetector error_detector_;
  GraphMetadata graph_metadata_;
};

TEST_F(ErrorDetectorTest, InitGCRSet) {
  error_detector_.InitGCRSet();
  EXPECT_EQ(io_manager_.GetGCRs().size(), 2);
  EXPECT_EQ(error_detector_.get_attributed_paths().size(), 3);

  auto paths = error_detector_.get_attributed_paths();

  auto path0 = error_detector_.get_attributed_paths()[0];
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

  auto path1 = error_detector_.get_attributed_paths()[1];
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

  auto path2 = error_detector_.get_attributed_paths()[2];
  EXPECT_EQ(path2.size(), 2);
  EXPECT_EQ(path2[0].label_id, 0);
  EXPECT_EQ(path2[0].attribute_ids.size(), 1);
  EXPECT_EQ(path2[0].attribute_ids[0], 1);
  EXPECT_EQ(path2[0].attribute_values[0], "70");
  EXPECT_EQ(path2[0].op_types[0], OpType::kGt);
  EXPECT_EQ(path2[1].label_id, 1);
  EXPECT_EQ(path2[1].attribute_ids.size(), 0);
}

}  // namespace sics::graph::miniclean::components::error_detector