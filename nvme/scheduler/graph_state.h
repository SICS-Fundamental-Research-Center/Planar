#ifndef GRAPH_SYSTEMS_NVME_SCHEDULER_GRAPH_STATE_H_
#define GRAPH_SYSTEMS_NVME_SCHEDULER_GRAPH_STATE_H_

#include <memory>
#include <vector>

#include "core/common/config.h"
#include "core/common/types.h"
#include "core/data_structures/graph/serialized_mutable_csr_graph.h"
#include "core/data_structures/serializable.h"
#include "core/data_structures/serialized.h"
#include "nvme/data_structures/graph/serialized_pram_block_csr.h"

namespace sics::graph::nvme::scheduler {

struct GraphState {
 public:
  typedef enum {
    OnDisk = 1,
    Reading,
    Serialized,
    Deserialized,
    Computed,
    Writing,
  } StorageStateType;

  GraphState() : memory_size_(64 * 1024){};
  GraphState(size_t num_subgraphs)
      : num_blocks_(num_subgraphs),
        memory_size_(core::common::Configurations::Get()->memory_size) {
    subgraph_storage_state_.resize(num_subgraphs, OnDisk);
    graphs_.resize(num_subgraphs);
    current_round_pending_.resize(num_subgraphs, true);
    block_mutate_state_.resize(num_subgraphs, false);
  }

  void ResetCurrentRoundPending() {
    // all blocks should be iterated in current round
    for (size_t i = 0; i < num_blocks_; ++i) {
      current_round_pending_.at(i) = true;
    }
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
  }

  void SetComputedSerializedToReadSerialized(core::common::GraphID gid) {
    subgraph_storage_state_.at(gid) = Serialized;
  }

  void SetGraphState(core::common::GraphID gid, StorageStateType type) {
    subgraph_storage_state_.at(gid) = type;
  }

  void SetCurrentRoundPendingFinish(core::common::GraphID gid) {
    current_round_pending_.at(gid) = false;
  }

  void SyncCurrentRoundPending() {
    for (size_t i = 0; i < num_blocks_; i++) {
      current_round_pending_.at(i) = true;
    }
  }

  // block mutate state functions

  void SetBlockMutated(core::common::GraphID bid) {
    block_mutate_state_.at(bid) = true;
  }

  bool IsBlockMutated(core::common::GraphID bid) const {
    return block_mutate_state_.at(bid);
  }

//  core::data_structures::Serializable* GetSubgraph(core::common::GraphID gid) {
//    return graphs_.at(gid).get();
//  }

//  void SetSubGraph(
//      core::common::GraphID gid,
//      std::unique_ptr<core::data_structures::Serializable> subgraph) {
//    graphs_.at(gid).swap(subgraph);
//  }

  // release unique_ptr of serializable graph
//  void ReleaseSubgraph(core::common::GraphID gid) { graphs_.at(gid).reset(); }

 public:
  size_t num_blocks_;
  std::vector<StorageStateType> subgraph_storage_state_;

  // label for if current round graph is executed
  std::vector<bool> current_round_pending_;
  std::vector<int> round_;
  std::vector<bool> block_mutate_state_;
  // memory size and graph size
  // TODO: memory size should be set by gflags

  const size_t memory_size_;

 private:
  std::vector<std::unique_ptr<core::data_structures::Serializable>> graphs_;
};
}  // namespace sics::graph::nvme::scheduler

#endif  // GRAPH_SYSTEMS_NVME_SCHEDULER_GRAPH_STATE_H_
