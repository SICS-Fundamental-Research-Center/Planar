#ifndef GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_PRAM_CSR_GRAPH_H_
#define GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_PRAM_CSR_GRAPH_H_

#include <memory>

#include "common/bitmap.h"
#include "common/bitmap_no_ownership.h"
#include "common/config.h"
#include "common/types.h"
#include "data_structures/graph/pram_block.h"
#include "data_structures/graph/serialized_pram_block_csr.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"
#include "util/atomic.h"
#include "util/pointer_cast.h"

namespace sics::graph::core::data_structures::graph {

template <typename TV, typename TE>
class PramCSRGraph : public Serializable {
  using GraphID = common::GraphID;
  using BlockID = common::BlockID;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexDegree = common::VertexDegree;
  using SerializedPramBlockCSRGraph =
      data_structures::graph::SerializedPramBlockCSRGraph;

 public:
  using VertexData = TV;
  using EdgeData = TE;

  PramCSRGraph() = default;
  explicit PramCSRGraph(BlockMetadata* blockmetadata) {}
};

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_PRAM_CSR_GRAPH_H_
