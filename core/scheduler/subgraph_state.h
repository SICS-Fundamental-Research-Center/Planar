#ifndef GRAPH_SYSTEMS_CORE_SCHEDULER_SUBGRAPH_STATE_H_
#define GRAPH_SYSTEMS_CORE_SCHEDULER_SUBGRAPH_STATE_H_

#include <cstdio>
#include <memory>
#include <vector>

#include "common/types.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"

namespace sics::graph::core::scheduler {

class GraphState {
 public:
  typedef enum {
    OnDisk = 1,
    Serialized,
    Deserialized,
  } StorageStateType;

  GraphState() = default;
  GraphState(size_t num_subgraphs) {
    num_subgraphs_ = num_subgraphs;
    subgraph_round_.resize(num_subgraphs, 0);
    subgraph_storage_state_.resize(num_subgraphs, OnDisk);
    serialized_.resize(num_subgraphs);
    graphs_.resize(num_subgraphs);
    current_round_pending_.resize(num_subgraphs, true);
    next_round_pending_.resize(num_subgraphs, false);
  }

  void Init() {
    current_round_pending_.resize(num_subgraphs_, true);
    next_round_pending_.resize(num_subgraphs_, false);
  }

  StorageStateType GetSubgraphState(common::GraphID gid) {
    return subgraph_storage_state_.at(gid);
  }

  size_t GetSubgraphRound(common::GraphID gid) {
    return subgraph_round_.at(gid);
  }

  void SetGraphState(common::GraphID gid, StorageStateType type) {
    subgraph_storage_state_.at(gid) = type;
  }

  common::GraphID GetNextReadGraphInCurrentRound() {
    for (int i = 0; i < num_subgraphs_; i++) {
      if (current_round_pending_.at(i)) {
        return i;
      }
    }
    return INVALID_GRAPH_ID;
  }

  common::GraphID GetNextReadGaphInNextRound() {
    for (int i = 0; i < num_subgraphs_; i++) {
      if (next_round_pending_.at(i)) {
        return i;
      }
    }
    return INVALID_GRAPH_ID;
  }

  bool IsFinalGraphInCurrentRound() {
    return GetNextReadGraphInCurrentRound() == INVALID_GRAPH_ID;
  }

  bool IsGoOn() {
    return (GetNextReadGraphInCurrentRound() != INVALID_GRAPH_ID) ||
           (GetNextReadGaphInNextRound() != INVALID_GRAPH_ID);
  }

  void SyncRoundState() { current_round_pending_.swap(next_round_pending_); }

  // graph handlers
  data_structures::Serialized* GetSubgraphSerialized(common::GraphID gid) {
    return serialized_.at(gid).get();
  }

  data_structures::Serializable* GetSubgraph(common::GraphID gid) {
    return graphs_.at(gid).get();
  }

  void SetSubgraphSerialized(common::GraphID gid,
                             data_structures::Serialized* serialized) {
    serialized_.at(gid).reset(serialized);
  }

  void SetGraph(common::GraphID gid, data_structures::Serializable* subgraph) {
    graphs_.at(gid).reset(subgraph);
  }

 private:
  size_t num_subgraphs_;
  std::vector<int> subgraph_round_;
  std::vector<StorageStateType> subgraph_storage_state_;

  std::vector<bool> current_round_pending_;
  std::vector<bool> next_round_pending_;

  std::vector<std::unique_ptr<data_structures::Serialized>> serialized_;
  std::vector<std::unique_ptr<data_structures::Serializable>> graphs_;
};
}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_CORE_SCHEDULER_SUBGRAPH_STATE_H_
