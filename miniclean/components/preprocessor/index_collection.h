#ifndef MINICLEAN_COMPONENTS_PREPROCESSOR_INDEX_COLLECTION_H_
#define MINICLEAN_COMPONENTS_PREPROCESSOR_INDEX_COLLECTION_H_

#include <yaml-cpp/yaml.h>

#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/util/logging.h"
#include "miniclean/common/types.h"

namespace sics::graph::miniclean::components::preprocessor {

class VertexAttributeSegment {
 private:
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
  using VertexAttributeValue =
      sics::graph::miniclean::common::VertexAttributeValue;
  using ValueBucket =
      std::unordered_map<VertexAttributeValue, std::vector<VertexID>>;
  using ValueBlock = std::vector<std::vector<VertexID>>;
  using AttributeBucket = std::unordered_map<VertexAttributeID, ValueBucket>;
  using AttributeBlock = std::unordered_map<VertexAttributeID, ValueBlock>;

 public:
  VertexAttributeSegment() = default;

  void Reserve(size_t num_bucket, size_t num_block) {
    bucket_by_value_.reserve(num_bucket);
    block_by_range_.reserve(num_block);
  }

  void LoadAttributeBucket(
      VertexLabel vlabel, const std::string& workspace_dir,
      const std::vector<VertexAttributeID>& attribute_id_list,
      const std::vector<std::string>& bucket_path_list,
      const std::vector<std::string>& bucket_size_path_list,
      const std::vector<std::string>& bucket_offset_path_list);

  void LoadAttributeBlock(
      VertexLabel vlabel, const std::string& workspace_dir,
      const std::vector<VertexAttributeID>& attribute_id_list,
      const std::vector<std::string>& block_path_list,
      const std::vector<std::string>& block_offset_path_list);

 private:
  template <typename T>
  void LoadBinFile(const std::string& bin_path, std::vector<T>& dst) {
    dst.clear();
    std::ifstream file(bin_path, std::ios::binary);
    if (!file) {
      LOG_FATAL("Error opening bin file: ", bin_path.c_str());
    }
    file.seekg(0, std::ios::end);
    size_t fileSize = file.tellg();
    file.seekg(0, std::ios::beg);

    dst.resize(fileSize / sizeof(T));

    file.read(reinterpret_cast<char*>(dst.data()), fileSize);
    file.close();
  }

  // `bucket by value` is a data structure that stores the vertex with the
  // same attribute value. The sizes of buckets could be significantly
  // different.
  //
  // This data structure is useful when the number attribute value is not that
  // large.
  std::unordered_map<VertexLabel, AttributeBucket> bucket_by_value_;

  // `block by range` is a data structure that stores the vertex with
  // similar attribute values. Two vertices with the same attribute value would
  // be stored in the same block. Specifically, the vertices would be sorted by
  // their attribute values and then be divided into several blocks. The sizes
  // of blocks is nearly uniform.
  //
  // This data structure is designed for the case when the number of attribute
  // is very large. By blocking the vertices with similar attribute values, we
  // can reduce the number of intersection operations and reduce the complexity
  // caused by cartesain product.
  std::unordered_map<VertexLabel, AttributeBlock> block_by_range_;
};

// Path pattern index is a data structure that stores both:
//   (1) vertex index by path pattern.
//   (2) pattern instance index by vertex.
class PathPatternIndex {
  // TODO: implement this.
};

class IndexCollection {
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
  IndexCollection() = default;
  void LoadIndexCollection(const std::string& path_pattern_file,
                           const std::string& attribute_config_file);

 private:
  void LoadPathPatterns(const std::string& path_pattern_file);
  void LoadAttributeConfig(const std::string& attribute_config_file);
  void LoadVertexAttributeSegment(const std::string& vertex_attribute_file);

  std::vector<PathPattern> path_patterns_;
  AttributeConfig attribute_config_;
  CategoricalAttributeBucket cate_attr_bucket_;
  VertexAttributeSegment vertex_attribute_segment_;
};

}  // namespace sics::graph::miniclean::components::preprocessor

namespace YAML {

using EdgePattern = sics::graph::miniclean::common::EdgePattern;
using PathPattern = sics::graph::miniclean::common::PathPattern;
using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using VertexAttributeSegment =
    sics::graph::miniclean::components::preprocessor::VertexAttributeSegment;
using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
using VertexID = sics::graph::miniclean::common::VertexID;

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
struct convert<VertexAttributeSegment> {
  static Node encode(const VertexAttributeSegment& vertex_attribute_segment) {
    // TODO: implement this.
    return Node();
  }
  static bool decode(const Node& node,
                     VertexAttributeSegment& vertex_attribute_segment) {
    Node bucket_node = node["AttributeBucketConfig"];
    Node block_node = node["AttributeBlockConfig"];
    vertex_attribute_segment.Reserve(bucket_node.size(), block_node.size());
    // Load bucket.
    for (size_t i = 0; i < bucket_node.size(); i++) {
      VertexLabel vlabel = bucket_node[i]["VertexLabel"].as<VertexLabel>();
      std::string workspace_path =
          bucket_node[i]["WorkspaceDirectory"].as<std::string>();
      std::vector<VertexAttributeID> attribute_id_list =
          bucket_node[i]["VertexAttributeIDList"]
              .as<std::vector<VertexAttributeID>>();
      std::vector<std::string> bucket_path_list =
          bucket_node[i]["BucketPathList"].as<std::vector<std::string>>();
      std::vector<std::string> bucket_size_path_list =
          bucket_node[i]["BucketSizePathList"].as<std::vector<std::string>>();
      std::vector<std::string> bucket_offset_path_list =
          bucket_node[i]["BucketOffsetPathList"].as<std::vector<std::string>>();
      vertex_attribute_segment.LoadAttributeBucket(
          vlabel, workspace_path, attribute_id_list, bucket_path_list,
          bucket_size_path_list, bucket_offset_path_list);
    }
    // Load block.
    for (size_t i = 0; i < block_node.size(); i++) {
      VertexLabel vlabel = block_node[i]["VertexLabel"].as<VertexLabel>();
      std::string workspace_path =
          block_node[i]["WorkspaceDirectory"].as<std::string>();
      std::vector<VertexAttributeID> attribute_id_list =
          block_node[i]["VertexAttributeIDList"]
              .as<std::vector<VertexAttributeID>>();
      std::vector<std::string> block_path_list =
          block_node[i]["BlockPathList"].as<std::vector<std::string>>();
      std::vector<std::string> block_offset_path_list =
          block_node[i]["BlockOffsetPathList"].as<std::vector<std::string>>();
      vertex_attribute_segment.LoadAttributeBlock(
          vlabel, workspace_path, attribute_id_list, block_path_list,
          block_offset_path_list);
    }
    return true;
  }
};
}  // namespace YAML

#endif  // MINICLEAN_COMPONENTS_PREPROCESSOR_INDEX_COLLECTION_H_