#ifndef GRAPH_SYSTEMS_CORE_SCHEDULER_GRAPH_STATE_H_
#define GRAPH_SYSTEMS_CORE_SCHEDULER_GRAPH_STATE_H_

#include <memory>
#include <vector>

#include "common/types.h"
#include "data_structures/graph/serialized_mutable_csr_graph.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"

namespace sics::graph::core::scheduler {

struct GraphState {
 public:
  typedef enum {
    OnDisk = 1,
    Reading,
    Serialized,
    Deserialized,
    Computed,
  } StorageStateType;

  GraphState() : memory_size_(64 * 1024){};
  GraphState(size_t num_subgraphs)
      : num_subgraphs_(num_subgraphs),
        memory_size_(common::Configurations::Get()->memory_size) {
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

  void SetOnDiskToSerialized(common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Serialized;
  }

  void SetOnDiskToReading(common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Reading;
  }

  void SetReadingToSerialized(common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Serialized;
  }

  void SetSerializedToDeserialized(common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Deserialized;
  }

  void SetDeserializedToComputed(common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Computed;
    current_round_pending_.at(gid) = false;
  }

  void SetComputedToSerialized(common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Serialized;
  }

  void SetSerializedToOnDisk(common::GraphID gid) {
    subgraph_storage_state_.at(gid) = OnDisk;
    subgraph_round_.at(gid) = subgraph_round_.at(gid) + 1;
  }

  void SetComputedSerializedToReadSerialized(common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Serialized;
    subgraph_round_.at(gid) = subgraph_round_.at(gid) + 1;
  }

  void UpdateSubgraphState2(common::GraphID gid, StorageStateType type) {
    subgraph_storage_state_.at(gid) = type;
    subgraph_round_.at(gid) = subgraph_round_.at(gid) + 1;
    current_round_pending_.at(gid) = false;
  }

  void SetGraphState(common::GraphID gid, StorageStateType type) {
    subgraph_storage_state_.at(gid) = type;
  }

  int GetSubgraphRound(common::GraphID gid) const {
    return subgraph_round_.at(gid);
  }

  void SetSubgraphRound(common::GraphID gid) {
    subgraph_round_.at(gid) = subgraph_round_.at(gid) + 1;
  }

  void SyncCurrentRoundPending() {
    for (int i = 0; i < num_subgraphs_; i++) {
      current_round_pending_.at(i) = true;
    }
  }

  // graph handlers
  data_structures::Serialized* GetSubgraphSerialized(common::GraphID gid) {
    return serialized_.at(gid).get();
  }

  data_structures::Serializable* GetSubgraph(common::GraphID gid) {
    return graphs_.at(gid).get();
  }

  data_structures::Serialized* NewSerializedMutableCSRGraph(
      common::GraphID gid) {
    serialized_.at(gid) =
        std::make_unique<data_structures::graph::SerializedMutableCSRGraph>();
    return serialized_.at(gid).get();
  }

  void SetSubgraphSerialized(
      common::GraphID gid,
      std::unique_ptr<data_structures::Serialized> serialized) {
    serialized_.at(gid).swap(serialized);
  }

  void SetSubGraph(common::GraphID gid,
                   std::unique_ptr<data_structures::Serializable> subgraph) {
    graphs_.at(gid).swap(subgraph);
  }

  void ReleaseSubgraphSerialized(common::GraphID gid) {
    serialized_.at(gid).reset();
  }

  void ReleaseSubgraph(common::GraphID gid) { graphs_.at(gid).reset(); }

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
  std::vector<size_t> subgraph_size_;
  const size_t memory_size_;
  size_t subgraph_limits_ = 1;

 private:
  std::vector<std::unique_ptr<data_structures::Serialized>> serialized_;
  std::vector<std::unique_ptr<data_structures::Serializable>> graphs_;
};
}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_CORE_SCHEDULER_GRAPH_STATE_H_
