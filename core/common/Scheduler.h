//
// Created by Yang Liu on 2023/7/27.
//

#ifndef GRAPH_SYSTEMS_SCHEDULER_H
#define GRAPH_SYSTEMS_SCHEDULER_H

#include "data_structures/graph_metadata.h"

namespace sics::graph::core::common {

class Scheduler {
 private:
  data_structures::GraphMetadata graph_metadata_;
  int current_round_ = 0;

 public:
  Scheduler();
  ~Scheduler() {}

  int GetCurrentRound() const { return current_round_; }
  void SetCurrentRound(int current_round) { current_round_ = current_round; }

  int GetSubgraphRound(GraphIDType subgraph_gid) const {
    if (graph_metadata_.IsSubgraphPendingCurrentRound(subgraph_gid)) {
      return current_round_
    } else {
      return current_round_ + 1;
    }
  }

      // read graph metadata from meta.yaml file
      // meta file path should be passed by gflags
      void
      ReadGraphMetadata(std::string graph_metadata_path) {
    YAML::Node graph_metadata_node;
    graph_metadata_node = YAML::LoadFile(graph_metadata_path);
    graph_metadata_ = graph_metadata_node["GraphMetadata"]
                         .as<data_structures::GraphMetadata>();
  }
};

}  // namespace sics::graph::core::common

#endif  // GRAPH_SYSTEMS_SCHEDULER_H
