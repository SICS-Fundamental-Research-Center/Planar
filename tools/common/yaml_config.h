#ifndef xyz_GRAPH_SYSTEMS_TOOLS_COMMON_YAML_CONFIG_H_
#define xyz_GRAPH_SYSTEMS_TOOLS_COMMON_YAML_CONFIG_H_

#include <yaml-cpp/yaml.h>

#include "core/data_structures/graph_metadata.h"
#include "core/util/logging.h"
#include "tools/common/data_structures.h"
#include "tools/common/types.h"

namespace YAML {

using xyz::graph::core::common::EdgeIndex;
using xyz::graph::core::common::VertexID;
using xyz::graph::core::data_structures::SubgraphMetadata;
using xyz::graph::tools::common::EdgelistMetadata;

template <>
struct convert<EdgelistMetadata> {
  static Node encode(EdgelistMetadata& metadata) {
    Node node;
    node["num_vertices"] = metadata.num_vertices;
    node["num_edges"] = metadata.num_edges;
    node["max_vid"] = metadata.max_vid;

    return node;
  }

  static bool decode(const Node& node, EdgelistMetadata& metadata) {
    if (node.size() != 3) {
      LOG_ERROR("Invalid edgelist metadata format");
      return false;
    }

    metadata.num_vertices = node["num_vertices"].as<VertexID>();
    metadata.num_edges = node["num_edges"].as<EdgeIndex>();
    metadata.max_vid = node["max_vid"].as<VertexID>();

    return true;
  }
};

}  // namespace YAML

#endif  // xyz_GRAPH_SYSTEMS_TOOLS_COMMON_YAML_CONFIG_H_
