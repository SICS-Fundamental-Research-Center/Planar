#ifndef GRAPH_SYSTEMS_CORE_SCHEDULER_GRAPH_MANAGER_H_
#define GRAPH_SYSTEMS_CORE_SCHEDULER_GRAPH_MANAGER_H_

#include <vector>

#include "common/types.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"
#include "scheduler/message_hub.h"

namespace sics::graph::core::scheduler {

template <typename TV, typename TE>
struct GraphManager {
  using VertexData = TV;
  using EdgeData = TE;
  using Graph = data_structures::graph::MutableBlockCSRGraph;
  using VertexID = common::VertexID;

  GraphManager() = default;
  GraphManager(const std::string& root_path, scheduler::MessageHub* hub)
      : metadata_(root_path) {
    auto num_subgraphs = metadata_.num_blocks;
    graphs_.resize(num_subgraphs);
    read_data_ = new VertexData[metadata_.num_vertices];
    write_data_ = new VertexData[metadata_.num_vertices];
    // Init info for scheduling.
    active_edge_blocks_.resize(num_subgraphs);
    is_in_memory_.resize(num_subgraphs);
    for (uint32_t i = 0; i < num_subgraphs; i++) {
      auto block = metadata_.blocks.at(i);
      active_edge_blocks_.at(i).Init(block.num_sub_blocks);
      is_in_memory_.at(i).resize(block.num_sub_blocks, false);
    }
  }
  ~GraphManager() {
    delete[] read_data_;
    delete[] write_data_;
  }

  Graph* GetGraph(common::GraphID gid) { return &graphs_.at(gid); }

  void Sync(bool read_only = false) {
    if (!read_only) {
      memcpy(read_data_, write_data_,
             metadata_.num_vertices * sizeof(VertexData));
    }
  }

  const common::Bitmap* GetActiveEdgeBlocks(common::GraphID gid) {
    return &active_edge_blocks_.at(gid);
  }

  const uint32_t GetNumBlock() { return metadata_.num_blocks; }

  const std::vector<VertexID>& GetActiveEdgeBlockInMemory(common::GraphID gid) {

  }

  const data_structures::SubBlock& GetBlockInfo(common::GraphID gid,
                                                common::BlockID sub_id) {
    return metadata_.blocks.at(gid).sub_blocks.at(sub_id);
  }

  // Active edge block for executing.
  void SetActiveEdgeBlock(common::GraphID gid) {
    active_gid_ = gid;
    active_edge_blocks_.at(gid).Fill();
  }

 public:
  data_structures::TwoDMetadata metadata_;

  // Vertex states.
  VertexData* read_data_ = nullptr;
  VertexData* write_data_ = nullptr;

  common::GraphID active_gid_ = 0;
  std::vector<common::Bitmap> active_edge_blocks_;
  // Keep the edge block in memory.
  // Or write back to disk for memory space release.
  std::vector<std::vector<bool>> is_in_memory_;
  // Edges structure.
  std::vector<Graph> graphs_;

  // Buffer size management.
  size_t buffer_size_;
};

}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_CORE_SCHEDULER_GRAPH_MANAGER_H_
