#ifndef GRAPH_SYSTEMS_CORE_SCHEDULER_SUBGRAPH_STATE_H_
#define GRAPH_SYSTEMS_CORE_SCHEDULER_SUBGRAPH_STATE_H_

#include <cstdio>
#include <vector>

#include "common/types.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"

namespace sics::graph::core::scheduler {

class SubgraphState {
 public:
  typedef enum {
    InDisk = 1,
    Serialized,
    Deserialized,
  } StorageStateType;

  SubgraphState() = default;
  SubgraphState(size_t num_subgraphs) {
    subgraph_round_.resize(num_subgraphs, 0);
    subgraph_storage_state_.resize(num_subgraphs, InDisk);
  }

  StorageStateType GetSubgraphState(common::GraphID gid) {
    return subgraph_storage_state_.at(gid);
  }

  size_t GetSubgraphRound(common::GraphID gid) {
    return subgraph_round_.at(gid);
  }

  data_structures::Serialized* GetSubgraphSerialized(common::GraphID gid) {
    return serialized_.at(gid).get();
  }

  data_structures::Serializable* GetSubgraph(common::GraphID gid) {
    return graphs_.at(gid).get();
  }

 private:
  std::vector<int> subgraph_round_;
  std::vector<StorageStateType> subgraph_storage_state_;

  std::vector<std::unique_ptr<data_structures::Serialized>> serialized_;
  std::vector<std::unique_ptr<data_structures::Serializable>> graphs_;
};
}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_CORE_SCHEDULER_SUBGRAPH_STATE_H_
