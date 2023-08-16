#ifndef GRAPH_SYSTEMS_CORE_SCHEDULER_GRAPH_STATE_H_
#define GRAPH_SYSTEMS_CORE_SCHEDULER_GRAPH_STATE_H_

#include <memory>
#include <vector>

#include "common/types.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"

namespace sics::graph::core::scheduler {

struct GraphState {
 public:
  typedef enum {
    OnDisk = 1,
    Serialized,
    Deserialized,
    Computed,
  } StorageStateType;

  GraphState() = default;
  GraphState(size_t num_subgraphs, size_t memory_size)
      : memory_size_(memory_size) {
    num_subgraphs_ = num_subgraphs;
    subgraph_round_.resize(num_subgraphs, 0);
    subgraph_storage_state_.resize(num_subgraphs, OnDisk);
    serialized_.resize(num_subgraphs);
    graphs_.resize(num_subgraphs);
    current_round_pending_.resize(num_subgraphs, true);
    next_round_pending_.resize(num_subgraphs, false);
  }

  StorageStateType GetSubgraphState(common::GraphID gid) const {
    return subgraph_storage_state_.at(gid);
  }

  void SetGraphState(common::GraphID gid, StorageStateType type) {
    subgraph_storage_state_.at(gid) = type;
  }

  size_t GetSubgraphRound(common::GraphID gid) const {
    return subgraph_round_.at(gid);
  }

  void SetSubgraphRound(common::GraphID gid, size_t round) {
    subgraph_round_.at(gid) = round;
  }

  void SyncRoundState() { current_round_pending_.swap(next_round_pending_); }

  // graph handlers
  data_structures::Serialized* GetSubgraphSerialized(common::GraphID gid) {
    return serialized_.at(gid).get();
  }

  data_structures::Serializable* GetSubgraph(common::GraphID gid) {
    return graphs_.at(gid).get();
  }

  void SetSubgraphSerialized(
      common::GraphID gid,
      std::unique_ptr<data_structures::Serialized> serialized) {
    serialized_.at(gid).swap(serialized);
  }

  void SetGraph(common::GraphID gid,
                std::unique_ptr<data_structures::Serializable> subgraph) {
    graphs_.at(gid).swap(subgraph);
  }

 public:
  size_t num_subgraphs_;
  std::vector<int> subgraph_round_;
  std::vector<StorageStateType> subgraph_storage_state_;

  // label for if current round graph is executed
  std::vector<bool> current_round_pending_;
  // label for if next round graph is executed
  std::vector<bool> next_round_pending_;

  // memory size and graph size
  // TODO: memory size should be set by gflags
  const size_t memory_size_;
  std::atomic_int subgraph_limits_ = 1;

 private:
  std::vector<std::unique_ptr<data_structures::Serialized>> serialized_;
  std::vector<std::unique_ptr<data_structures::Serializable>> graphs_;
};
}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_CORE_SCHEDULER_GRAPH_STATE_H_
