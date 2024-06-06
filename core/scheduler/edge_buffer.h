#ifndef GRAPH_SYSTEMS_EDGE_BUFFER_H
#define GRAPH_SYSTEMS_EDGE_BUFFER_H

#include <condition_variable>
#include <mutex>
#include <vector>

#include "common/config.h"
#include "common/types.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph_metadata.h"

namespace sics::graph::core::scheduler {

class EdgeBuffer {
  using GraphID = common::GraphID;
  using BlockID = common::BlockID;

 public:
  EdgeBuffer() = default;
  EdgeBuffer(data_structures::TwoDMetadata* meta)
      : buffer_size_(common::Configurations::Get()->edge_buffer_size) {}
  void Init(size_t size) { buffer_size_ = size; }

 public:
  void ApplyBuffer(GraphID gid, BlockID bid) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto size = edge_block_size_.at(gid).at(bid);
    while (size > buffer_size_) ReleaseUsedBuffer();
  }

  void ReleaseUsedBuffer() {
    // TODO: Get on used edge block.
    auto gid_used = 0;
    auto bid_used = 0;
  }

  void ReleaseBuffer(GraphID gid, BlockID bid) {}

 private:
  std::mutex mtx_;
  std::condition_variable cv_;

  size_t buffer_size_;
  std::vector<std::vector<size_t>> edge_block_size_;

  std::vector<std::vector<bool>> is_in_memory_;
  std::vector<std::vector<bool>> is_finished_;

  std::vector<data_structures::graph::MutableBlockCSRGraph>& graphs_;
};

}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_EDGE_BUFFER_H
