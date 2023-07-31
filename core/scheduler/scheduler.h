#ifndef GRAPH_SYSTEMS_SCHEDULER_H
#define GRAPH_SYSTEMS_SCHEDULER_H

#include "data_structures/graph_metadata.h"

namespace sics::graph::core::scheduler {

class scheduler {
 private:
  data_structures::GraphMetadata graph_metadata_;
  int current_round_ = 0;

 public:
  scheduler() = default;
  ~scheduler() = default;

  int GetCurrentRound() const { return current_round_; }
  void SetCurrentRound(int current_round) { current_round_ = current_round; }

  int GetSubgraphRound(common::GraphID subgraph_gid) const;

  // read graph metadata from meta.yaml file
  // meta file path should be passed by gflags
  void ReadGraphMetadata(std::string graph_metadata_path);
};

}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_SCHEDULER_H
