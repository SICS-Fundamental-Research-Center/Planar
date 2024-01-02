#include "miniclean/components/error_detector/error_detector.h"

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <string>

#include "miniclean/common/error_detection_config.h"
#include "miniclean/data_structures/graphs/miniclean_graph_metadata.h"

namespace sics::graph::miniclean::components::error_detector {

using ErrorDetectionConfig =
    sics::graph::miniclean::common::ErrorDetectionConfig;
using GraphMetadata =
    sics::graph::miniclean::data_structures::graphs::MiniCleanGraphMetadata;

class ErrorDetectorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    YAML::Node node;
    EXPECT_NO_THROW(
        node = YAML::LoadFile(
            data_dir +
            "/input/error_detector/graph/5_num/partition_result/meta.yaml"));
    graph_metadata_ = node.as<GraphMetadata>();
    ErrorDetectionConfig::Init(graph_metadata_);
  }
  std::string data_dir = TEST_DATA_DIR;
  ErrorDetector error_detector =
      ErrorDetector(data_dir + "/input/error_detector/");
  GraphMetadata graph_metadata_;
};

TEST_F(ErrorDetectorTest, InitGCRSet) { error_detector.InitGCRSet(); }

}  // namespace sics::graph::miniclean::components::error_detector