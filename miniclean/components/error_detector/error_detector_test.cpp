#include "miniclean/components/error_detector/error_detector.h"

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <string>

#include "core/common/multithreading/thread_pool.h"
#include "miniclean/common/error_detection_config.h"
#include "miniclean/data_structures/graphs/miniclean_graph.h"
#include "miniclean/data_structures/graphs/miniclean_graph_metadata.h"
#include "miniclean/io/miniclean_graph_reader.h"
#include "miniclean/messages/message.h"

namespace sics::graph::miniclean::components::error_detector {

using ErrorDetectionConfig =
    sics::graph::miniclean::common::ErrorDetectionConfig;
using GraphMetadata =
    sics::graph::miniclean::data_structures::graphs::MiniCleanGraphMetadata;
using OpType = sics::graph::miniclean::data_structures::gcr::OpType;
using MiniCleanGraph =
    sics::graph::miniclean::data_structures::graphs::MiniCleanGraph;
using SerializedMiniCleanGraph =
    sics::graph::miniclean::data_structures::graphs::SerializedMiniCleanGraph;
using ReadMessage = sics::graph::miniclean::messages::ReadMessage;
using Reader = sics::graph::miniclean::io::MiniCleanGraphReader;

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
  ErrorDetector error_detector(data_dir + "/input/error_detector/gcrs.yaml");
  error_detector.InitGCRSet();
  EXPECT_EQ(error_detector.GetGCRs().size(), 2);
  EXPECT_EQ(error_detector.GetAttributedPaths().size(), 7);

  auto paths = error_detector.GetAttributedPaths();

  auto path0 = error_detector.GetAttributedPaths()[0];
  EXPECT_EQ(path0.size(), 3);
  EXPECT_EQ(path0[0].label_id, 0);
  EXPECT_EQ(path0[0].attribute_ids.size(), 1);
  EXPECT_EQ(path0[0].attribute_ids[0], 3);
  EXPECT_EQ(path0[0].attribute_values[0], "Thriller");
  EXPECT_EQ(path0[0].op_types[0], OpType::kEq);
  EXPECT_EQ(path0[1].label_id, 1);
  EXPECT_EQ(path0[1].attribute_ids.size(), 0);
  EXPECT_EQ(path0[2].label_id, 0);
  EXPECT_EQ(path0[2].attribute_ids.size(), 0);

  auto path1 = error_detector.GetAttributedPaths()[1];
  EXPECT_EQ(path1.size(), 2);
  EXPECT_EQ(path1[0].label_id, 0);
  EXPECT_EQ(path1[0].attribute_ids.size(), 1);
  EXPECT_EQ(path1[0].attribute_ids[0], 3);
  EXPECT_EQ(path1[0].attribute_values[0], "Thriller");
  EXPECT_EQ(path1[0].op_types[0], OpType::kEq);
  EXPECT_EQ(path1[1].label_id, 1);
  EXPECT_EQ(path1[1].attribute_ids.size(), 0);

  auto path2 = error_detector.GetAttributedPaths()[2];
  EXPECT_EQ(path2.size(), 2);
  EXPECT_EQ(path2[0].label_id, 0);
  EXPECT_EQ(path2[0].attribute_ids.size(), 0);
  EXPECT_EQ(path2[1].label_id, 2);
  EXPECT_EQ(path2[1].attribute_ids.size(), 1);
  EXPECT_EQ(path2[1].attribute_ids[0], 12);
  EXPECT_EQ(path2[1].attribute_values[0], "'Big' James_Wroblewski");
  EXPECT_EQ(path2[1].op_types[0], OpType::kEq);

  auto path3 = error_detector.GetAttributedPaths()[3];
  EXPECT_EQ(path3.size(), 2);
  EXPECT_EQ(path3[0].label_id, 0);
  EXPECT_EQ(path3[0].attribute_ids.size(), 0);
  EXPECT_EQ(path3[1].label_id, 1);
  EXPECT_EQ(path3[1].attribute_ids.size(), 1);
  EXPECT_EQ(path3[1].attribute_ids[0], 10);
  EXPECT_EQ(path3[1].attribute_values[0], "'Aziz_Al-Na'ib");
  EXPECT_EQ(path3[1].op_types[0], OpType::kEq);

  auto path4 = error_detector.GetAttributedPaths()[4];
  EXPECT_EQ(path4.size(), 3);
  EXPECT_EQ(path4[0].label_id, 0);
  EXPECT_EQ(path4[0].attribute_ids.size(), 1);
  EXPECT_EQ(path4[0].attribute_ids[0], 2);
  EXPECT_EQ(path4[0].attribute_values[0], "2000");
  EXPECT_EQ(path4[0].op_types[0], OpType::kGt);
  EXPECT_EQ(path4[1].label_id, 2);
  EXPECT_EQ(path4[1].attribute_ids.size(), 0);
  EXPECT_EQ(path4[2].label_id, 0);
  EXPECT_EQ(path4[2].attribute_ids.size(), 0);

  auto path5 = error_detector.GetAttributedPaths()[5];
  EXPECT_EQ(path5.size(), 2);
  EXPECT_EQ(path5[0].label_id, 0);
  EXPECT_EQ(path5[0].attribute_ids.size(), 1);
  EXPECT_EQ(path5[0].attribute_ids[0], 2);
  EXPECT_EQ(path5[0].attribute_values[0], "2000");
  EXPECT_EQ(path5[0].op_types[0], OpType::kGt);
  EXPECT_EQ(path5[1].label_id, 1);
  EXPECT_EQ(path5[1].attribute_ids.size(), 0);

  auto path6 = error_detector.GetAttributedPaths()[6];
  EXPECT_EQ(path6.size(), 2);
  EXPECT_EQ(path6[0].label_id, 0);
  EXPECT_EQ(path6[0].attribute_ids.size(), 0);
  EXPECT_EQ(path6[1].label_id, 1);
  EXPECT_EQ(path6[1].attribute_ids.size(), 0);
}

}  // namespace sics::graph::miniclean::components::error_detector