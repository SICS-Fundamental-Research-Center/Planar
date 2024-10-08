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
  using GraphID = common::GraphID;

 public:
  typedef enum {
    OnDisk = 1,
    Reading,
    Serialized,
    Deserialized,
    InMemory,
    Computed,
  } StorageStateType;

  GraphState() : memory_size_(64 * 1024){};
  GraphState(size_t num_subgraphs) { Init(num_subgraphs); }

  void Init(size_t num_subgraphs) {
    num_subgraphs_ = num_subgraphs;
    auto mode = core::common::Configurations::Get()->mode;
    if (mode == common::Static) {
      bids_.resize(num_subgraphs);
      for (int i = 0; i < bids_.size(); i++) {
        bids_.at(i).push_back(i);
      }
    } else if (mode == common::Random) {
      bids_.resize(4);
      auto size = (num_subgraphs + 3) / 4;
      for (int i = 0; i < num_subgraphs; i++) {
        bids_.at(i / size).push_back(i);
      }
      num_subgraphs_ = 4;
    }
    is_loaded_.resize(num_subgraphs, false);
    memory_size_ = common::Configurations::Get()->memory_size;
    subgraph_round_.resize(num_subgraphs_, 0);
    subgraph_storage_state_.resize(num_subgraphs_, OnDisk);
    serialized_.resize(num_subgraphs_);
    graphs_.resize(num_subgraphs_);
    current_round_pending_.resize(num_subgraphs_, true);
    next_round_pending_.resize(num_subgraphs_, false);
    is_block_mode_ = common::Configurations::Get()->is_block_mode;
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

  void SetGraphCurrentRoundFinish(common::GraphID gid) {
    current_round_pending_.at(gid) = false;
  }

  void SetGraphState(common::GraphID gid, StorageStateType type) {
    subgraph_storage_state_.at(gid) = type;
  }

  int GetSubgraphRound(common::GraphID gid) const {
    return subgraph_round_.at(gid);
  }

  void AddGraphRound(common::GraphID gid) {
    subgraph_round_.at(gid) = subgraph_round_.at(gid) + 1;
  }

  void SyncCurrentRoundPending() {
    for (size_t i = 0; i < num_subgraphs_; i++) {
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

  // allocate new Serialized graph for reader. This function will create
  // corresponding type Serialized graph.
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
  // release unique_ptr of serialized graph
  void ReleaseSubgraphSerialized(common::GraphID gid) {
    serialized_.at(gid).reset();
  }
  // release unique_ptr of serializable graph
  void ReleaseSubgraph(common::GraphID gid) { graphs_.at(gid).reset(); }

  bool IsEdgesLoaded(common::GraphID gid) { return is_loaded_.at(gid); }

  void SetEdgeLoaded(common::GraphID gid) { is_loaded_.at(gid) = true; }

  void ReleaseEdges(common::GraphID gid) { is_loaded_.at(gid) = false; }

  size_t GetSubBlockNum(common::GraphID gid) { return bids_.at(gid).size(); }

  const std::vector<common::BlockID>& GetSubBlockIDs(common::GraphID gid) {
    return bids_.at(gid);
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
  std::vector<size_t> subgraph_size_;
  size_t memory_size_;
  size_t subgraph_limits_ = 1;
  bool is_block_mode_ = false;

  // Used for Static and Random mode
  std::vector<std::vector<common::BlockID>> bids_;
  std::vector<bool> is_loaded_;

 private:
  std::vector<std::unique_ptr<data_structures::Serialized>> serialized_;
  std::vector<std::unique_ptr<data_structures::Serializable>> graphs_;
};
}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_CORE_SCHEDULER_GRAPH_STATE_H_
