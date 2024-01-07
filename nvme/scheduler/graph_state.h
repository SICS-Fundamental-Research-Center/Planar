#ifndef GRAPH_SYSTEMS_NVME_SCHEDULER_GRAPH_STATE_H_
#define GRAPH_SYSTEMS_NVME_SCHEDULER_GRAPH_STATE_H_

#include <memory>
#include <vector>

#include "core/common/config.h"
#include "core/common/types.h"
#include "core/data_structures/graph/serialized_mutable_csr_graph.h"
#include "core/data_structures/graph/serialized_pram_block_csr.h"
#include "core/data_structures/serializable.h"
#include "core/data_structures/serialized.h"

namespace sics::graph::nvme::scheduler {

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
        memory_size_(core::common::Configurations::Get()->memory_size) {
    subgraph_round_.resize(num_subgraphs, 0);
    subgraph_storage_state_.resize(num_subgraphs, OnDisk);
    serialized_.resize(num_subgraphs);
    graphs_.resize(num_subgraphs);
    current_round_pending_.resize(num_subgraphs, true);
    next_round_pending_.resize(num_subgraphs, false);
    is_block_mode_ = core::common::Configurations::Get()->is_block_mode;
  }

  StorageStateType GetSubgraphState(core::common::GraphID gid) const {
    return subgraph_storage_state_.at(gid);
  }

  void SetOnDiskToSerialized(core::common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Serialized;
  }

  void SetOnDiskToReading(core::common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Reading;
  }

  void SetReadingToSerialized(core::common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Serialized;
  }

  void SetSerializedToDeserialized(core::common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Deserialized;
  }

  void SetDeserializedToComputed(core::common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Computed;
    current_round_pending_.at(gid) = false;
  }

  void SetComputedToSerialized(core::common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Serialized;
  }

  void SetSerializedToOnDisk(core::common::GraphID gid) {
    subgraph_storage_state_.at(gid) = OnDisk;
    subgraph_round_.at(gid) = subgraph_round_.at(gid) + 1;
  }

  void SetComputedSerializedToReadSerialized(core::common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Serialized;
    subgraph_round_.at(gid) = subgraph_round_.at(gid) + 1;
  }

  void UpdateSubgraphState2(core::common::GraphID gid, StorageStateType type) {
    subgraph_storage_state_.at(gid) = type;
    subgraph_round_.at(gid) = subgraph_round_.at(gid) + 1;
    current_round_pending_.at(gid) = false;
  }

  void SetGraphState(core::common::GraphID gid, StorageStateType type) {
    subgraph_storage_state_.at(gid) = type;
  }

  int GetSubgraphRound(core::common::GraphID gid) const {
    return subgraph_round_.at(gid);
  }

  void SetSubgraphRound(core::common::GraphID gid) {
    subgraph_round_.at(gid) = subgraph_round_.at(gid) + 1;
  }

  void SyncCurrentRoundPending() {
    for (int i = 0; i < num_subgraphs_; i++) {
      current_round_pending_.at(i) = true;
    }
  }

  // graph handlers
  core::data_structures::Serialized* GetSubgraphSerialized(
      core::common::GraphID gid) {
    return serialized_.at(gid).get();
  }

  core::data_structures::Serializable* GetSubgraph(core::common::GraphID gid) {
    return graphs_.at(gid).get();
  }

  // allocate new Serialized graph for reader. This function will create
  // corresponding type Serialized graph.
  core::data_structures::Serialized* NewSerializedMutableCSRGraph(
      core::common::GraphID gid) {
    if (is_block_mode_) {
      serialized_.at(gid) = std::make_unique<
          core::data_structures::graph::SerializedPramBlockCSRGraph>();
    } else {
      serialized_.at(gid) = std::make_unique<
          core::data_structures::graph::SerializedMutableCSRGraph>();
      return serialized_.at(gid).get();
    }
  }

  // allocate new Serialized block_nvme graph for reader.
  core::data_structures::Serialized* NewSerializedBlockGraph(
      core::common::GraphID gid) {
    serialized_.at(gid) = std::make_unique<
        core::data_structures::graph::SerializedPramBlockCSRGraph>();
    return serialized_.at(gid).get();
  }

  void SetSubgraphSerialized(
      core::common::GraphID gid,
      std::unique_ptr<core::data_structures::Serialized> serialized) {
    serialized_.at(gid).swap(serialized);
  }

  void SetSubGraph(
      core::common::GraphID gid,
      std::unique_ptr<core::data_structures::Serializable> subgraph) {
    graphs_.at(gid).swap(subgraph);
  }
  // release unique_ptr of serialized graph
  void ReleaseSubgraphSerialized(core::common::GraphID gid) {
    serialized_.at(gid).reset();
  }
  // release unique_ptr of serializable graph
  void ReleaseSubgraph(core::common::GraphID gid) { graphs_.at(gid).reset(); }

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
  bool is_block_mode_ = false;

 private:
  std::vector<std::unique_ptr<core::data_structures::Serialized>> serialized_;
  std::vector<std::unique_ptr<core::data_structures::Serializable>> graphs_;
};
}  // namespace sics::graph::nvme::scheduler

#endif  // GRAPH_SYSTEMS_NVME_SCHEDULER_GRAPH_STATE_H_
