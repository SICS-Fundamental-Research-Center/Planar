#ifndef SICS_GRAPH_SYSTEMS_TOOLS_COMMON_YAML_CONFIG_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_COMMON_YAML_CONFIG_H_

#include <yaml-cpp/yaml.h>

#include "common/tools_types.h"
#include "common/types.h"

using sics::graph::core::common::GraphID;
using sics::graph::core::common::VertexID;
using tools::common::SubgraphMetaData;

namespace YAML {

template <>
struct convert<SubgraphMetaData> {
  static Node encode(const SubgraphMetaData& rhs) {
    Node node;
    node["gid"] = rhs.gid;
    node["num_vertices"] = rhs.num_vertices;
    node["num_incoming_edges"] = rhs.num_incoming_edges;
    node["num_outgoing_edges"] = rhs.num_outgoing_edges;
    node["max_vid"] = rhs.max_vid;
    node["min_vid"] = rhs.min_vid;
    return node;
  }

  static bool decode(const Node& node, SubgraphMetaData& rhs) {
    rhs.gid = node["gid"].as<GraphID>();
    rhs.num_vertices = node["num_vertices"].as<size_t>();
    rhs.num_incoming_edges = node["num_incoming_edges"].as<size_t>();
    rhs.num_outgoing_edges = node["num_outgoing_edges"].as<size_t>();
    rhs.max_vid = node["max_vid"].as<VertexID>();
    rhs.min_vid = node["min_vid"].as<VertexID>();
    return true;
  }
};

}  // namespace YAML

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_COMMON_YAML_CONFIG_H_
