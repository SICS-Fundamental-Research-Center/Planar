#include "graph_metadata.h"

namespace sics::graph::core::data_structures {

// TODO: initialize GraphMetadata by item not copy construction
GraphMetadata::GraphMetadata(const std::string& graph_metadata_path) {
  YAML::Node metadata_node;
  try {
    metadata_node = YAML::LoadFile(graph_metadata_path);

  } catch (YAML::BadFile& e) {
    LOG_ERROR("meta.yaml file read failed! ", e.msg);
  }
}

void GraphMetadata::Init() {
  // round 0 will load all subgraphs
  for (int i = 0; i < num_subgraphs_; i++) {
    current_round_pending_.push_back(true);
  }
}

common::GraphID GraphMetadata::GetNextLoadGraph() {
  for (int i = 0; i < num_subgraphs_; ++i) {
    if (current_round_pending_.at(i)) {
      return i;
    }
  }
  return INVALID_GRAPH_ID;
}

void GraphMetadata::SyncNextRound() {
  current_round_pending_.swap(next_round_pending_);
  //  current_round_pending_ = next_round_pending_;
  //  next_round_pending_.clear();
  //  next_round_pending_.resize(num_subgraphs_, false);
}

}  // namespace sics::graph::core::data_structures