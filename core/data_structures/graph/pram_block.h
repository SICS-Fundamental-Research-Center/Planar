#ifndef GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_PRAM_BLOCK_H_
#define GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_PRAM_BLOCK_H_

#include <memory>

#include "common/bitmap.h"
#include "common/bitmap_no_ownership.h"
#include "common/config.h"
#include "common/types.h"
#include "data_structures/graph/serialized_mutable_csr_graph.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"
#include "util/atomic.h"
#include "util/pointer_cast.h"

namespace sics::graph::core::data_structures::graph {

template <typename TV, typename TE>
class PramBlock : public Serializable {
  using GraphID = common::GraphID;
  using BlockID = common::BlockID;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexDegree = common::VertexDegree;
  using SerializedMutableCSRGraph =
      data_structures::graph::SerializedMutableCSRGraph;

 public:
  using VertexData = TV;
  using EdgeData = TE;
  PramBlock() = default;
  explicit PramBlock(BlockID block_id) : blockId_(block_id) {}

  // TODO: add block methods like sub-graph

 private:
  BlockID blockId_;
  VertexID begin_;
  VertexID end_;

  VertexID num_block_vertices_;
  EdgeIndex num_out_egdes_;

  uint8_t* block_buf_base_;
  VertexID* vid_by_index_;  // controlled by block
  VertexDegree* out_degree_base_;
  EdgeIndex* out_offset_base_;
  VertexID* out_edges_base_;

  // no data here, use global data only

  // used for PRAM algorithm, which needs to delete edge
  VertexDegree* out_degree_base_new_;
  EdgeIndex* out_offset_base_new_;
  VertexID* out_edges_base_new_;
  common::Bitmap edge_delete_bitmap_;

};
}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_PRAM_BLOCK_H_
