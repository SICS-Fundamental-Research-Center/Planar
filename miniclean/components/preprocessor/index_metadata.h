#ifndef MINICLEAN_COMPONENTS_PREPROCESSOR_INDEX_METADATA_H_
#define MINICLEAN_COMPONENTS_PREPROCESSOR_INDEX_METADATA_H_

#include <yaml-cpp/yaml.h>

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "core/util/logging.h"
#include "miniclean/common/types.h"

namespace sics::graph::miniclean::components::preprocessor {

class IndexMetadata {
 private:
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;

 public:
  IndexMetadata() = default;
  IndexMetadata(
      std::unordered_map<VertexLabel, std::vector<std::pair<uint8_t, uint8_t>>>
          attribute_metadata)
      : attribute_metadata_(attribute_metadata) {}

  const std::unordered_map<VertexLabel,
                           std::vector<std::pair<uint8_t, uint8_t>>>&
  get_attribute_bitmap() const {
    return attribute_metadata_;
  }

 private:
  // The first element of the pair is the attribute id.
  // The second element of the pair is the number of the attribute value.
  std::unordered_map<VertexLabel, std::vector<std::pair<uint8_t, uint8_t>>>
      attribute_metadata_;
};

}  // namespace sics::graph::miniclean::components::preprocessor

namespace YAML {
template <>
struct convert<
    sics::graph::miniclean::components::preprocessor::IndexMetadata> {
  static Node encode(
      const sics::graph::miniclean::components::preprocessor::IndexMetadata&
          index_metadata) {
    Node node;
    // TODO (bai-wenchao): implement it when needed.
    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::miniclean::components::preprocessor::IndexMetadata&
          index_metadata) {
    std::unordered_map<sics::graph::miniclean::common::VertexLabel,
                       std::vector<std::pair<uint8_t, uint8_t>>>
        attribute_metadata;
    auto attribute_metadata_nodes = node["AttributeMetadata"];
    attribute_metadata.reserve(attribute_metadata_nodes.size());

    for (const auto attribute_metadata_node : attribute_metadata_nodes) {
      sics::graph::miniclean::common::VertexLabel vertex_label =
          static_cast<sics::graph::miniclean::common::VertexLabel>(
              std::stoi(attribute_metadata_node.first.as<std::string>()));

      std::vector<std::pair<uint8_t, uint8_t>> attribute;
      attribute.reserve(attribute_metadata_node.second.size());
      for (const auto pair_node : attribute_metadata_node.second) {
        uint8_t attribute_id = static_cast<uint8_t>(
            std::stoi(pair_node["attribute_id"].as<std::string>()));
        uint8_t attribute_num = static_cast<uint8_t>(
            std::stoi(pair_node["attribute_number"].as<std::string>()));
        attribute.emplace_back(attribute_id, attribute_num);
      }
      attribute_metadata[vertex_label] = attribute;
    }

    index_metadata =
        sics::graph::miniclean::components::preprocessor::IndexMetadata(
            attribute_metadata);

    return true;
  }
};
}  // namespace YAML

#endif  // MINICLEAN_COMPONENTS_PREPROCESSOR_INDEX_METADATA_H_