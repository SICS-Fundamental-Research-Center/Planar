#ifndef GRAPH_SYSTEMS_EDGE_BUFFER_H
#define GRAPH_SYSTEMS_EDGE_BUFFER_H

#include <condition_variable>
#include <mutex>
#include <queue>
#include <vector>

#include "common/blocking_queue.h"
#include "common/config.h"
#include "common/types.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph_metadata.h"

namespace sics::graph::core::scheduler {

class EdgeBuffer {
  using GraphID = common::GraphID;
  using BlockID = common::BlockID;
  using VertexID = common::VertexID;

 public:
  EdgeBuffer() = default;
  explicit EdgeBuffer(
      data_structures::TwoDMetadata* meta,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs) {
    Init(meta, graphs);
  }

  void Init(data_structures::TwoDMetadata* meta,
            std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs) {
    buffer_size_ = common::Configurations::Get()->edge_buffer_size;
    graphs_ = graphs;
    edge_block_size_.resize(meta->num_blocks);
    is_reading_.resize(meta->num_blocks);
    is_in_memory_.resize(meta->num_blocks);
    is_finished_.resize(meta->num_blocks);
    for (GraphID i = 0; i < meta->num_blocks; i++) {
      auto block_meta = meta->blocks.at(i);
      for (BlockID j = 0; j < block_meta.num_sub_blocks; j++) {
        auto size = sizeof(VertexID) * block_meta.sub_blocks.at(j).num_edges;
        max_block_size_ = std::max(max_block_size_, size);
        edge_block_size_.at(i).push_back(size);
      }
      is_reading_.at(i).resize(block_meta.num_sub_blocks, false);
      is_in_memory_.at(i).resize(block_meta.num_sub_blocks, false);
      is_finished_.at(i).resize(block_meta.num_sub_blocks, false);
    }
  }

 public:
  void ApplyBuffer(GraphID gid, BlockID bid) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto size = edge_block_size_.at(gid).at(bid);
    while (size > buffer_size_) ReleaseUsedBuffer();
    buffer_size_ -= size;
    is_reading_.at(gid).at(bid) = true;
  }

  bool IsBufferNotEnough(GraphID gid, BlockID bid) {
    return buffer_size_ < edge_block_size_.at(gid).at(bid);
  }

  // Blocking when no edge block can be release.
  void ReleaseUsedBuffer() {
    // TODO: Get on used edge block.
    auto entry = used_edge_blocks_.PopOrWait();
    ReleaseBuffer(entry.first, entry.second);
  }

  void ReleaseBuffer(GraphID gid, BlockID bid) {
    graphs_->at(gid).Release(bid);
  }

  // TODO: useful??
  void SetBlockReading(GraphID gid, BlockID bid) {
    is_reading_.at(gid).at(bid);
  }

  void PushOneEdgeBlock(GraphID gid, BlockID bid) {
    loaded_blocks_.emplace(gid, bid);
    is_in_memory_.at(gid).at(bid) = true;
  }

  std::pair<GraphID, BlockID> PopOneEdgeBlock() {
    auto res = loaded_blocks_.front();
    loaded_blocks_.pop();
    return res;
  }

  // Check state for blocks.

  bool IsInMemory(GraphID gid, BlockID bid) {
    return is_in_memory_.at(gid).at(bid);
  }

  bool IsFinished(GraphID gid, BlockID bid) {
    return is_finished_.at(gid).at(bid);
  }

  void UseOneEdgeBlock(GraphID gid, BlockID bid) {
    is_finished_.at(gid).at(bid) = true;
    used_edge_blocks_.Push(std::pair<GraphID, BlockID>(gid, bid));
  }

  std::vector<BlockID> GetBlocksInMemory(GraphID gid) {
    std::vector<BlockID> res;
    for (BlockID bid = 0; bid < is_active_.at(gid).size(); bid++) {
      if (is_active_[gid][bid] && is_in_memory_.at(gid).at(bid))
        res.push_back(bid);
    }
    return res;
  }

  std::vector<BlockID> GetBlocksToRead(GraphID gid) {
    std::vector<BlockID> res;
    for (BlockID bid = 0; bid < is_active_.at(gid).size(); bid++) {
      if (is_active_[gid][bid] && !is_in_memory_.at(gid).at(bid))
        res.push_back(bid);
    }
    return res;
  }

 private:
  std::mutex mtx_;
  std::condition_variable cv_;

  size_t max_block_size_ = 0;
  size_t buffer_size_;
  std::vector<std::vector<size_t>> edge_block_size_;

  std::vector<std::vector<bool>> is_active_;
  std::vector<std::vector<bool>> is_reading_;
  std::vector<std::vector<bool>> is_in_memory_;
  std::vector<std::vector<bool>> is_finished_;

  common::BlockingQueue<std::pair<GraphID, BlockID>> used_edge_blocks_;
  std::queue<std::pair<GraphID, BlockID>> loaded_blocks_;
  std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs_;
};

}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_EDGE_BUFFER_H
