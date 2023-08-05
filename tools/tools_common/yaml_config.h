#ifndef SICS_GRAPH_SYSTEMS_TOOLS_COMMON_YAML_CONFIG_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_COMMON_YAML_CONFIG_H_

#include <yaml-cpp/yaml.h>

#include "common/types.h"
#include "data_structures/graph_metadata.h"
#include "tools_common/data_structures.h"
#include "util/logging.h"

// TO DO: add support for edgelist graph
namespace YAML {

using sics::graph::core::common::GraphID;
using sics::graph::core::common::VertexID;
using sics::graph::core::data_structures::SubgraphMetadata;
using tools::EdgelistMetadata;

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

    metadata.num_vertices = node["num_vertices"].as<size_t>();
    metadata.num_edges = node["num_edges"].as<size_t>();
    metadata.max_vid = node["max_vid"].as<VertexID>();

    return true;
  }
};

}  // namespace YAML

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_COMMON_YAML_CONFIG_H_
