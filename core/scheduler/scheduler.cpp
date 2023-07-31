#include "scheduler.h"

namespace sics::graph::core::scheduler {

int scheduler::GetSubgraphRound(common::GraphID subgraph_gid) const {
  if (graph_metadata_.IsSubgraphPendingCurrentRound(subgraph_gid)) {
    return current_round_;
  } else {
    return current_round_ + 1;
  }
}

void scheduler::ReadGraphMetadata(std::string graph_metadata_path) {
  YAML::Node graph_metadata_node;
  graph_metadata_node = YAML::LoadFile(graph_metadata_path);
  graph_metadata_ =
      graph_metadata_node["GraphMetadata"].as<data_structures::GraphMetadata>();
}

}  // namespace sics::graph::core::scheduler