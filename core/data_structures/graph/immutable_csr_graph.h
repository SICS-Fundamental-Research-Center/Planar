#ifndef CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_H_
#define CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_H_

#include <memory>
#include <cmath>
#include <list>
#include <utility>

#include "util/logging.h"
#include "common/types.h"
#include "data_structures/graph/serialized_immutable_csr_graph.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"

namespace sics::graph::core::data_structures::graph {

class ImmutableCSRGraph : public Serializable {
 private:
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;

 public:
  explicit ImmutableCSRGraph(const GraphID gid) : Serializable(), gid_(gid) {}

  std::unique_ptr<Serialized> Serialize(const common::TaskRunner& runner) override;

  void Deserialize(const common::TaskRunner& runner,
                   Serialized&& serialized) override;

  GraphID get_gid() const { return gid_; }
  void set_gid(GraphID gid) { gid_ = gid; }

  void printSubgraph();

 private:
  void ParseSubgraphCSR(const std::list<OwnedBuffer>& buffer_list);

  GraphID gid_ = -1;
  uint8_t* buf_graph_ = nullptr;

  // serialized data in CSR format.
  VertexID* localid_by_globalid_ = nullptr;
  VertexID* valid_localid_by_globalid_ = nullptr;
  VertexID* localid_by_index_ = nullptr;
  VertexID* globalid_by_index_ = nullptr;
  VertexID* in_edges_ = nullptr;
  VertexID* out_edges_ = nullptr;
  size_t* indegree_ = nullptr;
  size_t* outdegree_ = nullptr;
  size_t* in_offset_ = nullptr;
  size_t* out_offset_ = nullptr;
};

}  // namespace sics::graph::core::data_structures::graph

#endif  // CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_H_
