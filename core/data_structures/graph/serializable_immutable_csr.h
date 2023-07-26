#ifndef GRAPH_SYSTEMS_SERIALIZABLE_IMMUTABLE_CSR_H
#define GRAPH_SYSTEMS_SERIALIZABLE_IMMUTABLE_CSR_H

#include <memory>

#include "common/types.h"
#include "data_structures/graph/serialized_immutable_csr.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"

namespace sics::graph::core::data_structures::graph {

class SerializableImmutableCSR : public Serializable {
 private:
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;

 public:
  SerializableImmutableCSR(const GraphID gid, const VertexID max_vid)
      : Serializable(), gid_(gid), max_vid_(max_vid) {}

  std::unique_ptr<Serialized> Serialize(common::TaskRunner& runner) override;

  void Deserialize(common::TaskRunner& runner,
                   Serialized&& serialized) override;

  inline GraphID get_gid() const { return gid_; }
  inline void set_gid(GraphID gid) { gid_ = gid; }

 private:
  void ParseSubgraphCSR(std::list<OwnedBuffer>& buffer_list);

  GraphID gid_ = -1;
  VertexID* buf_graph_ = nullptr;

  VertexID max_vid_ = 0;
  VertexID aligned_max_vid_ = 0;
  SerializedImmutableCSR serialized_immutable_csr_;

  // serialized data in CSR format.
  VertexID* localid_by_globalid_ = nullptr;
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

#endif  // GRAPH_SYSTEMS_SERIALIZABLE_IMMUTABLE_CSR_H