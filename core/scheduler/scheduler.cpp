#include "scheduler.h"

namespace sics::graph::core::scheduler {

template <typename VertexData, typename EdgeData>
int Scheduler<VertexData, EdgeData>::GetSubgraphRound(common::GraphID subgraph_gid) const {
  if (graph_metadata_info_.IsSubgraphPendingCurrentRound(subgraph_gid)) {
    return current_round_;
  } else {
    return current_round_ + 1;
  }
}

template <typename VertexData, typename EdgeData>
void Scheduler<VertexData, EdgeData>::ReadGraphMetadata(const std::string& graph_metadata_path) {
  YAML::Node graph_metadata_node;
  try {
    graph_metadata_node = YAML::LoadFile(graph_metadata_path);
    graph_metadata_info_ = graph_metadata_node["GraphMetadata"]
                               .as<data_structures::GraphMetadata>();
  } catch (YAML::BadFile& e) {
    LOG_ERROR("meta.yaml file read failed! ", e.msg);
  }
}

}  // namespace sics::graph::core::scheduler