#include "miniclean/components/preprocessor/attribute_bucket.h"

namespace sics::graph::miniclean::components::preprocessor {

void AttributeBucket::InitCategoricalAttributeBucket(
    const std::string& path_pattern_file,
    const std::string& attribute_config_file) {
  LoadPathPatterns(path_pattern_file);
  LoadAttributeConfig(attribute_config_file);

  cate_attr_bucket_.resize(path_patterns_.size());
  for (size_t i = 0; i < path_patterns_.size(); ++i) {
    size_t pattern_size = path_patterns_[i].size();
    cate_attr_bucket_[i].resize(pattern_size);
    for (size_t j = 0; j < pattern_size; ++j) {
      VertexLabel vlabel = std::get<0>(path_patterns_[i][j]);
      size_t cate_attr_num = attribute_config_[vlabel][0].size();
      cate_attr_bucket_[i][j].resize(cate_attr_num);
      for (size_t k = 0; k < cate_attr_num; ++k) {
        size_t min_attr_val = attribute_config_[vlabel][0][k].first;
        size_t max_attr_val = attribute_config_[vlabel][0][k].second;
        cate_attr_bucket_[i][j][k].resize(max_attr_val - min_attr_val + 1);
      }
    }
  }
}

void AttributeBucket::LoadPathPatterns(const std::string& path_pattern_file) {
  YAML::Node path_pattern_config = YAML::LoadFile(path_pattern_file);
  YAML::Node path_pattern_node = path_pattern_config["PathPatterns"];
  path_patterns_ = path_pattern_node.as<std::vector<PathPattern>>();
}

void AttributeBucket::LoadAttributeConfig(
    const std::string& attribute_config_file) {
  YAML::Node attribute_config = YAML::LoadFile(attribute_config_file);
  YAML::Node attribute_config_node = attribute_config["AttributeConfig"];
  attribute_config_ = attribute_config_node.as<AttributeConfig>();
}

}  // namespace sics::graph::miniclean::components::preprocessor