#include "miniclean/data_structures/gcr/light_gcr.h"

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <fstream>
#include <vector>

#include "miniclean/common/error_detection_config.h"
#include "miniclean/data_structures/graphs/miniclean_graph_metadata.h"

namespace sics::graph::miniclean::data_structures::gcr {

using ErrorDetectionConfig =
    sics::graph::miniclean::common::ErrorDetectionConfig;
using MiniCleanGraphMetadata =
    sics::graph::miniclean::data_structures::graphs::MiniCleanGraphMetadata;

class LightGCRTest : public ::testing::Test {
 protected:
  void SetUp() override {
    YAML::Node graph_metadata;
    EXPECT_NO_THROW(
        graph_metadata = YAML::LoadFile(
            data_dir_ +
            "/input/miniclean_graph_reader/partition_result/meta.yaml"));
    graph_metadata_ = graph_metadata.as<MiniCleanGraphMetadata>();
    ErrorDetectionConfig::Init(graph_metadata_);

    // 1. Left star pattern.
    //    path 0: Movie [genre=Comedy, year>2000] -> Cast
    //    path 1: Movie [genre=Comedy, year>2000] -> Cast
    AttributedVertex left_center =
        ErrorDetectionConfig::Get()->GetLabelIDByName("Movie");
    left_center.attribute_ids.push_back(
        ErrorDetectionConfig::Get()->GetAttrIDByName("movie_genre"));
    left_center.attribute_values.push_back("Comedy");
    left_center.op_types.push_back(OpType::kEq);
    left_center.attribute_ids.push_back(
        ErrorDetectionConfig::Get()->GetAttrIDByName("movie_year"));
    left_center.attribute_values.push_back("2000");
    left_center.op_types.push_back(OpType::kGt);
    AttributedVertex actor0(
        ErrorDetectionConfig::Get()->GetLabelIDByName("Cast"));
    AttributedVertex actor1(
        ErrorDetectionConfig::Get()->GetLabelIDByName("Cast"));
    gcr_0_.set_left_star_pattern(
        {{left_center, actor0}, {left_center, actor1}});
    gcr_1_.set_left_star_pattern(
        {{left_center, actor0}, {left_center, actor1}});
    // 2. Right star pattern.
    //    path 0: Movie [rating>70] -> Director[name="James Cameron"]
    //    path 1: Movie [rating>70] -> Cast
    AttributedVertex right_center =
        ErrorDetectionConfig::Get()->GetLabelIDByName("Movie");
    right_center.attribute_ids.push_back(
        ErrorDetectionConfig::Get()->GetAttrIDByName("movie_rating"));
    right_center.attribute_values.push_back("70");
    right_center.op_types.push_back(OpType::kGt);
    AttributedVertex director(
        ErrorDetectionConfig::Get()->GetLabelIDByName("Director"));
    director.attribute_ids.push_back(
        ErrorDetectionConfig::Get()->GetAttrIDByName("director_name"));
    director.attribute_values.push_back("James Cameron");
    director.op_types.push_back(OpType::kEq);
    AttributedVertex actor(
        ErrorDetectionConfig::Get()->GetLabelIDByName("Cast"));
    gcr_0_.set_right_star_pattern(
        {{right_center, director}, {right_center, actor}});
    gcr_1_.set_right_star_pattern(
        {{right_center, director}, {right_center, actor}});
    // 3. Binary predicates.
    //    pred 0: left movie name = right movie name
    //    pred 1: left cast name = right cast name
    BinaryPredicate pred0(
        true, false, 0, 0, 0, 0,
        ErrorDetectionConfig::Get()->GetAttrIDByName("movie_name"),
        ErrorDetectionConfig::Get()->GetAttrIDByName("movie_name"),
        OpType::kEq);
    BinaryPredicate pred1(
        true, false, 0, 1, 1, 1,
        ErrorDetectionConfig::Get()->GetAttrIDByName("cast_name"),
        ErrorDetectionConfig::Get()->GetAttrIDByName("cast_name"), OpType::kEq);
    gcr_0_.set_binary_preconditions({pred0, pred1});
    gcr_1_.set_binary_preconditions({pred0, pred1});
    // 4. Consequence.
    //    cons 0: left movie vid = right movie vid
    //    cons 1: left movie year = 2018
    Consequence bin_conseq(
        true, false, 0, 0, 0, 0,
        ErrorDetectionConfig::Get()->GetAttrIDByName("movie_vid"),
        ErrorDetectionConfig::Get()->GetAttrIDByName("movie_vid"), OpType::kEq);
    Consequence uny_conseq(
        true, 0, 0, ErrorDetectionConfig::Get()->GetAttrIDByName("movie_year"),
        "2018", OpType::kEq);
    gcr_0_.set_consequence(bin_conseq);
    gcr_1_.set_consequence(uny_conseq);
  }

  // Shared data structures for test cases.
  MiniCleanGraphMetadata graph_metadata_;
  LightGCR gcr_0_;
  LightGCR gcr_1_;
  std::string data_dir_ = TEST_DATA_DIR;
};

TEST_F(LightGCRTest, TestDecoding) {
  // Save the test sample to file. (Test encoding)
  YAML::Node gcr_set;
  gcr_set["GCRs"] = std::vector<LightGCR>{gcr_0_, gcr_1_};
  YAML::Emitter emitter;
  std::ofstream fout(
      data_dir_ + std::string("/input/miniclean_light_gcr_parser/gcrs.yaml"));
  emitter << gcr_set;
  fout << emitter.c_str();
  fout.close();

  // Load the test yaml file.
  YAML::Node gcrs;
  EXPECT_NO_THROW(
      gcrs = YAML::LoadFile(
          data_dir_ +
          std::string("/input/miniclean_light_gcr_parser/gcrs.yaml")));

  // Test the decoding.
  std::vector<LightGCR> gcr_set_decoded =
      gcrs["GCRs"].as<std::vector<LightGCR>>();

  // Test GCR 0: left pattern
  EXPECT_EQ(gcr_set_decoded[0].get_left_star_pattern().size(),
            gcr_0_.get_left_star_pattern().size());
  for (size_t i = 0; i < gcr_0_.get_left_star_pattern().size(); i++) {
    EXPECT_EQ(gcr_set_decoded[0].get_left_star_pattern()[i].size(),
              gcr_0_.get_left_star_pattern()[i].size());
    for (size_t j = 0; j < gcr_0_.get_left_star_pattern()[i].size(); j++) {
      EXPECT_EQ(gcr_set_decoded[0].get_left_star_pattern()[i][j].label_id,
                gcr_0_.get_left_star_pattern()[i][j].label_id);
      EXPECT_EQ(gcr_set_decoded[0].get_left_star_pattern()[i][j].attribute_ids,
                gcr_0_.get_left_star_pattern()[i][j].attribute_ids);
      EXPECT_EQ(
          gcr_set_decoded[0].get_left_star_pattern()[i][j].attribute_values,
          gcr_0_.get_left_star_pattern()[i][j].attribute_values);
      EXPECT_EQ(gcr_set_decoded[0].get_left_star_pattern()[i][j].op_types,
                gcr_0_.get_left_star_pattern()[i][j].op_types);
    }
  }

  // Test attribute convert.
  std::string attr_value =
      gcr_set_decoded[0].get_left_star_pattern()[0][0].attribute_values[1];
  EXPECT_EQ(attr_value, "2000");
}
}  // namespace sics::graph::miniclean::data_structures::gcr