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
    Computed,
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

  void SetGraphState(common::GraphID gid, StorageStateType type) {
    subgraph_storage_state_.at(gid) = type;
  }

  size_t GetSubgraphRound(common::GraphID gid) {
    return subgraph_round_.at(gid);
  }

  void SetSubgraphRound(common::GraphID gid, size_t round) {
    subgraph_round_.at(gid) = round;
  }

  common::GraphID GetNextReadGraphInCurrentRound() {
    for (int gid = 0; gid < num_subgraphs_; gid++) {
      if (current_round_pending_.at(gid) &&
          subgraph_storage_state_.at(gid) == OnDisk) {
        return gid;
      }
    }
    return INVALID_GRAPH_ID;
  }

  // get next execute graph in current round
  common::GraphID GetNextExecuteGraph() {
    for (int gid = 0; gid < num_subgraphs_; gid++) {
      if (current_round_pending_.at(gid) &&
          subgraph_storage_state_.at(gid) == Deserialized) {
        return gid;
      }
    }
    return INVALID_GRAPH_ID;
  }

  common::GraphID GetNextReadGraphInNextRound() {
    for (int gid = 0; gid < num_subgraphs_; gid++) {
      if (next_round_pending_.at(gid) &&
          subgraph_storage_state_.at(gid) == OnDisk) {
        return gid;
      }
    }
    return INVALID_GRAPH_ID;
  }

  bool IsFinalGraphInCurrentRound() {
    return GetNextReadGraphInCurrentRound() == INVALID_GRAPH_ID;
  }

  bool IsGoOn() {
    return (GetNextReadGraphInCurrentRound() != INVALID_GRAPH_ID) ||
           (GetNextReadGraphInNextRound() != INVALID_GRAPH_ID);
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
  size_t num_subgraphs_{};
  std::vector<int> subgraph_round_;
  std::vector<StorageStateType> subgraph_storage_state_;

  // label for if current round graph is executed
  std::vector<bool> current_round_pending_;
  // label for if next round graph is executed
  std::vector<bool> next_round_pending_;

  std::vector<std::unique_ptr<data_structures::Serialized>> serialized_;
  std::vector<std::unique_ptr<data_structures::Serializable>> graphs_;
};
}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_CORE_SCHEDULER_SUBGRAPH_STATE_H_
