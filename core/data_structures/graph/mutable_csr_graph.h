#ifndef GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_CSR_GRAPH_H_
#define GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_CSR_GRAPH_H_

#include "data_structures/graph_metadata.h"
#include "data_structures/serializable.h"
#include "common/types.h"


namespace sics::graph::core::data_structures::graph {

template <typename VertexData, typename EdgeData>
class MutableCSRGraph : public Serializable {
  using VertexID = common::VertexID;
  using GraphID = common::GraphID;
  using
  using EdgeIndex = common::EdgeIndex;

 public:
  explicit MutableCSRGraph(SubgraphMetadata metadata) : metadata_(metadata) {}

 private:
  SubgraphMetadata metadata_;

  uint8_t* graph_buf_base_ptr_;


};

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_CSR_GRAPH_H_
