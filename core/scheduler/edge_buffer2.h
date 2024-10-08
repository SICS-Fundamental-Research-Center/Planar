#ifndef GRAPH_SYSTEMS_CORE_SCHEDULER_EDGE_BUFFER2_H
#define GRAPH_SYSTEMS_CORE_SCHEDULER_EDGE_BUFFER2_H

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

class EdgeBuffer2 {
  using GraphID = common::GraphID;
  using BlockID = common::BlockID;
  using VertexID = common::VertexID;

 public:
  EdgeBuffer2() = default;
  explicit EdgeBuffer2(
      data_structures::TwoDMetadata* meta,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs) {
    Init(meta, graphs);
  }

  void Init(data_structures::TwoDMetadata* meta,
            std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs) {
    meta_ = meta;
    buffer_size_ = common::Configurations::Get()->edge_buffer_size;
    graphs_ = graphs;
    edge_block_size_.resize(meta->num_blocks);
    is_active_.resize(meta->num_blocks);
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
      is_active_.at(i).resize(block_meta.num_sub_blocks, true);
      is_reading_.at(i).resize(block_meta.num_sub_blocks, false);
      is_in_memory_.at(i).resize(block_meta.num_sub_blocks, false);
      is_finished_.at(i).resize(block_meta.num_sub_blocks, false);
    }
  }

 public:
  void Reset() {
    auto num_blocks = is_in_memory_.size();
    for (int i = 0; i < num_blocks; i++) {
      auto num_sub_blocks = is_in_memory_.at(i).size();
      is_in_memory_.at(i).resize(num_sub_blocks, false);
      is_reading_.at(i).resize(num_sub_blocks, false);
      is_finished_.at(i).resize(num_sub_blocks, false);
    }
  }

  void ApplyBufferBlock(GraphID gid, BlockID bid) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto size = edge_block_size_.at(gid).at(bid);
    while (size > buffer_size_) ReleaseUsedBuffer();
    buffer_size_ -= size;
    is_reading_.at(gid).at(bid) = true;
  }

  void ApplyBuffer(GraphID gid, BlockID bid) {
    std::lock_guard<std::mutex> lock(mtx_);
    buffer_size_ -= edge_block_size_.at(gid).at(bid);
    is_reading_.at(gid).at(bid) = true;
  }

  bool IsBufferNotEnough(GraphID gid, BlockID bid) {
    return buffer_size_ < edge_block_size_.at(gid).at(bid);
  }

  void WaitForBufferRelease() {
    std::unique_lock<std::mutex> lock(mtx_);
    cv_.wait(lock, [this]() { return !buffer_block_; });
  }

  // Blocking when no edge block can be release.
  void ReleaseUsedBuffer() {
    // TODO: Get on used edge block.
    auto entry = used_edge_blocks_.PopOrWait();
    ReleaseBuffer(entry.first, entry.second);
  }

  void ReleaseBuffer(GraphID gid) {
    std::lock_guard<std::mutex> lock(mtx_);
    for (int i = 0; i < meta_->blocks.at(gid).num_sub_blocks; i++) {
      graphs_->at(gid).Release(i);
      is_in_memory_.at(gid).at(i) = false;
      buffer_size_ += edge_block_size_.at(gid).at(i);
    }
    graphs_->at(gid).SetSubBlocksRelease();
  }

  void ReleaseBuffer(GraphID gid, BlockID bid) {
    std::lock_guard<std::mutex> lock(mtx_);
    graphs_->at(gid).Release(bid);
    is_in_memory_.at(gid).at(bid) = false;
    buffer_size_ += edge_block_size_.at(gid).at(bid);
  }

  // TODO: useful??
  void SetBlockReading(GraphID gid) {
    auto& block_meta = meta_->blocks.at(gid);
    for (BlockID i = 0; i < block_meta.num_sub_blocks; i++) {
      is_reading_.at(gid).at(i) = true;
    }
  }

  void Push(BlockID bid) { queue_.Push(bid); }

  void PushOneEdgeBlock(GraphID gid, BlockID bid) {
    queue_.Push(bid);
    is_in_memory_.at(gid).at(bid) = true;
  }

  common::BlockingQueue<common::BlockID>* GetQueue() { return &queue_; }

  // Check state for blocks.

  bool IsInMemory(GraphID gid, BlockID bid) {
    return is_in_memory_.at(gid).at(bid);
  }

  bool IsFinished(GraphID gid, BlockID bid) {
    return is_finished_.at(gid).at(bid);
  }

  void FinishOneEdgeBlock(GraphID gid, BlockID bid) {
    std::lock_guard<std::mutex> lock(mtx_);
    is_finished_.at(gid).at(bid) = true;
    //    used_edge_blocks_.Push(std::pair<GraphID, BlockID>(gid, bid));
    // Now, when finish one just release it.
    graphs_->at(gid).Release(bid);
    is_in_memory_.at(gid).at(bid) = false;
    buffer_size_ += edge_block_size_.at(gid).at(bid);
    //    std::unique_lock<std::mutex> lock(mtx_);
    //    buffer_block_ = false;
    //    cv_.notify_all();
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

  void SetBufferBlock() {
    std::lock_guard<std::mutex> lck(mtx_);
    buffer_block_ = true;
  }

  size_t GetBufferSize() { return buffer_size_ / 1024 / 1024; }

  void AccumulateRead(size_t size) {
    size_read_ += size;
  }

  size_t GetAccumulateSize() {
    return size_read_;
  }

 private:
  std::mutex mtx_;
  std::condition_variable cv_;

  data_structures::TwoDMetadata* meta_;

  bool buffer_block_ = false;

  size_t size_read_ = 0;

  size_t max_block_size_ = 0;
  size_t buffer_size_;
  std::vector<std::vector<size_t>> edge_block_size_;

  std::vector<std::vector<bool>> is_active_;
  std::vector<std::vector<bool>> is_reading_;
  std::vector<std::vector<bool>> is_in_memory_;
  std::vector<std::vector<bool>> is_finished_;

  common::BlockingQueue<std::pair<GraphID, BlockID>> used_edge_blocks_;

  common::BlockingQueue<common::BlockID> queue_;
  std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs_;
};

}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_CORE_SCHEDULER_EDGE_BUFFER2_H
