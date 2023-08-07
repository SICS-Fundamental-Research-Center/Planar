#ifndef CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_H_
#define CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_H_

#include <memory>
#include <cmath>
#include <list>
#include <utility>

#include "common/types.h"
#include "util/logging.h"
#include "data_structures/graph/immutable_csr_graph_config.h"
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
  ImmutableCSRGraph(const GraphID gid, ImmutableCSRGraphConfig&& csr_config)
      : Serializable(), gid_(gid), csr_config_(std::move(csr_config)) {}

  virtual std::unique_ptr<Serialized> Serialize(const common::TaskRunner& runner) override;

  virtual void Deserialize(const common::TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override;

  GraphID get_gid() const { return gid_; }
  void set_gid(GraphID gid) { gid_ = gid; }

  uint8_t* GetGraphBuffer() const { return buf_graph_; }
  VertexID* GetGlobalIDByIndex() const { return globalid_by_index_; }
  VertexID* GetLocalIDByIndex() const { return localid_by_index_; }
  VertexID* GetLocalIDByGlobalID() const { return localid_by_globalid_; }
  VertexID* GetInEdges() const { return in_edges_; }
  VertexID* GetOutEdges() const { return out_edges_; }
  size_t* GetInDegree() const { return indegree_; }
  size_t* GetOutDegree() const { return outdegree_; }
  size_t* GetInOffset() const { return in_offset_; }
  size_t* GetOutOffset() const { return out_offset_; }

 protected:
  virtual void ParseSubgraphCSR(const std::list<OwnedBuffer>& buffer_list);

  std::unique_ptr<SerializedImmutableCSRGraph> serialized_;
  GraphID gid_ = 0;
  uint8_t* buf_graph_ = nullptr;

 private:
  // config. attributes to build the CSR.
  ImmutableCSRGraphConfig csr_config_;

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

#endif  // CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_H_
