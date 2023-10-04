#ifndef MINICLEAN_COMPONENTS_PREPROCESSOR_ATTRIBUTE_BUCKET_H_
#define MINICLEAN_COMPONENTS_PREPROCESSOR_ATTRIBUTE_BUCKET_H_

#include <yaml-cpp/yaml.h>

#include <string>
#include <vector>

#include "core/util/logging.h"
#include "miniclean/common/types.h"

namespace sics::graph::miniclean::components::preprocessor {

class AttributeBucket {
 private:
  // The first dimension is the vertex label.
  // The second dimension indicates whether cate attr or val attr.
  // The third dimension stores the range of the attribute.
  using AttributeConfig =
      std::vector<std::vector<std::vector<std::pair<size_t, size_t>>>>;
  using PathPattern = sics::graph::miniclean::common::PathPattern;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;
  // The first dimension is the path pattern id.
  // The second dimension is the vertex index within the path pattern.
  // The third dimension is the attribute id.
  // The fourth dimension is the attribute value.
  // The fifth dimension stores the src vertex ids of paths that satisfy the
  //     pattern and the attribute.
  using CategoricalAttributeBucket =
      std::vector<std::vector<std::vector<std::vector<VertexID>>>>;

 public:
  AttributeBucket() = default;
  void InitCategoricalAttributeBucket(const std::string& path_pattern_file,
                                      const std::string& attribute_config_file);

 private:
  void LoadPathPatterns(const std::string& path_pattern_file);
  void LoadAttributeConfig(const std::string& attribute_config_file);

  std::vector<PathPattern> path_patterns_;
  AttributeConfig attribute_config_;
  CategoricalAttributeBucket cate_attr_bucket_;
};

}  // namespace sics::graph::miniclean::components::preprocessor

namespace YAML {

using EdgePattern = sics::graph::miniclean::common::EdgePattern;
using PathPattern = sics::graph::miniclean::common::PathPattern;
using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using AttributeConfig =
    std::vector<std::vector<std::vector<std::pair<size_t, size_t>>>>;

template <>
struct convert<PathPattern> {
  static Node encode(const PathPattern& path_pattern) {
    std::vector<uint8_t> path_pattern_node;
    path_pattern_node.reserve(path_pattern.size() * 2 - 1);
    for (size_t i = 0; i < path_pattern.size() - 1; ++i) {
      path_pattern_node.push_back(std::get<0>(path_pattern[i]));
      path_pattern_node.push_back(std::get<1>(path_pattern[i]));
    }
    path_pattern_node.push_back(
        std::get<0>(path_pattern[path_pattern.size() - 1]));
    Node node(path_pattern_node);
    return node;
  }

  static bool decode(const Node& node, PathPattern& path_pattern) {
    if (!node.IsSequence()) {
      return false;
    }
    path_pattern.reserve(node.size() / 2 + 1);
    for (size_t i = 0; i < node.size() - 1; i += 2) {
      path_pattern.emplace_back(node[i].as<VertexLabel>(),
                                node[i + 1].as<EdgeLabel>(),
                                node[i + 2].as<VertexLabel>());
    }
    path_pattern.emplace_back(node[node.size() - 1].as<VertexLabel>(),
                              MAX_EDGE_LABEL, MAX_VERTEX_LABEL);
    return true;
  }
};

template <>
struct convert<AttributeConfig> {
  static Node encode(const AttributeConfig& attribute_config) {
    // TODO: implement this.
    return Node();
  }
  static bool decode(const Node& node, AttributeConfig& attribute_config) {
    attribute_config.resize(node.size());
    for (size_t i = 0; i < node.size(); ++i) {
      attribute_config[i].resize(2);
      Node cate_config = node[i]["categorical"];
      if (cate_config.size() > 0) {
        attribute_config[i][0].resize(cate_config.size());
        for (size_t j = 0; j < cate_config.size(); ++j) {
          if (cate_config[j].size() != 2) return false;
          attribute_config[i][0][j].first = cate_config[j][0].as<size_t>();
          attribute_config[i][0][j].second = cate_config[j][1].as<size_t>();
        }
      }

      Node val_config = node[i]["value"];
      if (val_config.size() > 0) {
        attribute_config[i][1].resize(val_config.size());
        for (size_t j = 0; j < val_config.size(); ++j) {
          if (val_config[j].size() != 2) return false;
          attribute_config[i][1][j].first = val_config[j][0].as<size_t>();
          attribute_config[i][1][j].second = val_config[j][1].as<size_t>();
        }
      }
    }

    return true;
  }
};
}  // namespace YAML

#endif  // MINICLEAN_COMPONENTS_PREPROCESSOR_ATTRIBUTE_BUCKET_H_