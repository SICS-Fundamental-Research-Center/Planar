#ifndef GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_MEMORY_BUFFER_H
#define GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_MEMORY_BUFFER_H

#include <condition_variable>
#include <mutex>
#include <queue>
#include <vector>

#include "core/common/blocking_queue.h"
#include "core/common/config.h"
#include "core/common/types.h"
#include "core/data_structures/graph_metadata.h"
#include "nvme/data_structures/graph/block_csr_graph.h"

namespace sics::graph::nvme::data_structures {

class EdgeBuffer {
  using GraphID = core::common::GraphID;
  using BlockID = core::common::BlockID;
  using VertexID = core::common::VertexID;

 public:
  EdgeBuffer() = default;
  explicit EdgeBuffer(core::data_structures::BlockMetadata* meta,
                      data_structures::graph::BlockCSRGraph* graph)
      : thread_queues_(core::common::Configurations::Get()->parallelism) {
    Init(meta, graph);
  }

  void Init(core::data_structures::BlockMetadata* meta,
            data_structures::graph::BlockCSRGraph* graph) {
    meta_ = meta;
    buffer_size_ = core::common::Configurations::Get()->edge_buffer_size;
    thread_size_ = core::common::Configurations::Get()->parallelism;
    graph_ = graph;
    edge_block_size_.resize(meta->num_subBlocks);
    is_active_.resize(meta->num_subBlocks);
    is_reading_.resize(meta->num_subBlocks);
    is_in_memory_.resize(meta->num_subBlocks);
    is_finished_.resize(meta->num_subBlocks);
    for (BlockID i = 0; i < meta->num_subBlocks; i++) {
      auto block_meta = meta->subBlocks.at(i);
      auto size = sizeof(VertexID) * block_meta.num_edges;
      max_block_size_ = std::max(max_block_size_, size);
      edge_block_size_.push_back(size);
    }
    is_active_.resize(meta->num_subBlocks, true);
    is_reading_.resize(meta->num_subBlocks, false);
    is_in_memory_.resize(meta->num_subBlocks, false);
    is_finished_.resize(meta->num_subBlocks, false);
  }

 public:
  void Reset() {
    auto num_sub_blocks = is_in_memory_.size();
    is_in_memory_.resize(num_sub_blocks, false);
    is_reading_.resize(num_sub_blocks, false);
    is_finished_.resize(num_sub_blocks, false);
  }

  void ApplyBufferBlock(BlockID bid) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto size = edge_block_size_.at(bid);
    while (size > buffer_size_) ReleaseUsedBuffer();
    buffer_size_ -= size;
    is_reading_.at(bid) = true;
  }

  void ApplyBuffer(BlockID bid) {
    std::lock_guard<std::mutex> lock(mtx_);
    buffer_size_ -= edge_block_size_.at(bid);
    is_reading_.at(bid) = true;
  }

  bool IsBufferNotEnough(BlockID bid) {
    return buffer_size_ < edge_block_size_.at(bid);
  }

  void WaitForBufferRelease() {
    std::unique_lock<std::mutex> lock(mtx_);
    cv_.wait(lock, [this]() { return !buffer_block_; });
  }

  // Blocking when no edge block can be release.
  void ReleaseUsedBuffer() {
    // TODO: Get on used edge block.
    auto bid = used_edge_blocks_.PopOrWait();
    ReleaseSubBlock(bid);
  }

  void ReleaseAllSubBlock(GraphID gid) {
    std::lock_guard<std::mutex> lock(mtx_);
    for (int i = 0; i < meta_->num_subBlocks; i++) {
      graph_->Release(i);
      is_in_memory_.at(i) = false;
      buffer_size_ += edge_block_size_.at(i);
    }
    graph_->SetSubBlocksRelease();
  }

  void ReleaseSubBlock(BlockID bid) {
    std::lock_guard<std::mutex> lock(mtx_);
    graph_->Release(bid);
    is_in_memory_.at(bid) = false;
    buffer_size_ += edge_block_size_.at(bid);
  }

  // TODO: useful??
  void SetBlockReading(GraphID gid) {
    for (BlockID i = 0; i < meta_->num_subBlocks; i++) {
      is_reading_.at(i) = true;
    }
  }

  bool IsNeedLoad() { return true; }

  // Check state for blocks.

  bool IsInMemory(BlockID bid) { return is_in_memory_.at(bid); }

  bool IsFinished(BlockID bid) { return is_finished_.at(bid); }

  void FinishOneSubBlock(BlockID bid) {
    std::lock_guard<std::mutex> lock(mtx_);
    is_finished_.at(bid) = true;
    //    used_edge_blocks_.Push(std::pair<GraphID, BlockID>(gid, bid));
    // Now, when finish one just release it.
    graph_->Release(bid);
    is_in_memory_.at(bid) = false;
    buffer_size_ += edge_block_size_.at(bid);
    //    std::unique_lock<std::mutex> lock(mtx_);
    //    buffer_block_ = false;
    //    cv_.notify_all();
  }

  std::vector<BlockID> GetBlocksInMemory() {
    std::vector<BlockID> res;
    for (BlockID bid = 0; bid < meta_->num_subBlocks; bid++) {
      if (is_active_[bid] && is_in_memory_.at(bid)) res.push_back(bid);
    }
    return res;
  }

  std::vector<BlockID> GetBlocksToRead() {
    std::vector<BlockID> res;
    for (BlockID bid = 0; bid < meta_->num_subBlocks; bid++) {
      if (is_active_[bid] && !is_in_memory_.at(bid)) res.push_back(bid);
    }
    return res;
  }

  void SetBufferBlock() {
    std::lock_guard<std::mutex> lck(mtx_);
    buffer_block_ = true;
  }

  size_t GetBufferSize() { return buffer_size_ / 1024 / 1024; }

  void AccumulateRead(size_t size) { size_read_ += size; }

  size_t GetAccumulateSize() { return size_read_; }

  data_structures::graph::BlockCSRGraph* GetGraphPtr() { return graph_; }

  core::data_structures::BlockMetadata* GetMetaPtr() { return meta_; }

  std::vector<core::common::BlockingQueue<BlockID>>* GetThreadQueue() {
    return &thread_queues_;
  }

  const core::common::BlockingQueue<BlockID>& GetThreadQueue(uint32_t tid) {
    return thread_queues_.at(tid);
  }

  void LoadFinish(BlockID bid) {
    loaded_queue_.push_back(bid);
    is_in_memory_.at(bid) = true;
  }

  void SendToExecutor() {
    for (int i = 0; i < loaded_queue_.size(); i++) {
      thread_queues_.at(thread_index_++).Push(loaded_queue_.at(i));
    }
    thread_index_ = thread_index_ % thread_size_;
    loaded_queue_.clear();
  }

  // Communication with Loader

  // Load edge block not in memory
  void LoadBlocksNotInMemory() {
    //    for (int i = 0; i < bids.size(); i++) {
    //      queue_to_read_.push(bids[i]);
    //    }
    cv_to_read_.notify_all();
  }

  void StopLoader() {
    running_ = false;
    cv_to_read_.notify_all();
  }

  bool CheckIfWaitForLoading() {
    std::unique_lock<std::mutex> lck(mtx_to_read_);
    //    cv_to_read_.wait(lck, [this]() { return queue_to_read_.size() != 0;
    //    });
    cv_to_read_.wait(lck);
    return running_;
  }

 private:
  bool running_ = true;

  std::mutex mtx_;
  std::condition_variable cv_;

  core::data_structures::BlockMetadata* meta_;

  bool buffer_block_ = false;

  size_t size_read_ = 0;

  size_t max_block_size_ = 0;
  size_t buffer_size_;
  std::vector<size_t> edge_block_size_;

  std::vector<bool> is_active_;
  std::vector<bool> is_reading_;
  std::vector<bool> is_in_memory_;
  std::vector<bool> is_finished_;

  core::common::BlockingQueue<BlockID> used_edge_blocks_;

  // Communication with loader.
  std::mutex mtx_to_read_;
  std::condition_variable cv_to_read_;
  std::queue<BlockID> queue_to_read_;

  std::vector<BlockID> loaded_queue_;

  uint32_t thread_index_ = 0;
  uint32_t thread_size_ = 10;
  std::vector<core::common::BlockingQueue<BlockID>> thread_queues_;

  data_structures::graph::BlockCSRGraph* graph_;
};

}  // namespace sics::graph::nvme::data_structures

#endif  // GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_MEMORY_BUFFER_H
