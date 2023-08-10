#ifndef CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_H_
#define CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_H_

#include <cmath>
#include <list>
#include <memory>
#include <utility>

#include "common/types.h"
#include "data_structures/graph/immutable_csr_graph_config.h"
#include "data_structures/graph/serialized_immutable_csr_graph.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"
#include "util/logging.h"

namespace sics::graph::core::data_structures::graph {

struct ImmutableCSRVertex {
  using VertexID = sics::graph::core::common::VertexID;

  VertexID vid;
  size_t indegree;
  size_t outdegree;
  VertexID* in_edges = nullptr;
  VertexID* out_edges = nullptr;
};

class ImmutableCSRGraph : public Serializable {
 public:
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;

  explicit ImmutableCSRGraph(const GraphID gid) : Serializable(), gid_(gid) {}
  ImmutableCSRGraph(const GraphID gid, ImmutableCSRGraphConfig&& csr_config)
      : Serializable(), gid_(gid), csr_config_(std::move(csr_config)) {}

  ~ImmutableCSRGraph() {
    if (localid_by_globalid_ != nullptr) delete localid_by_globalid_;
    if (globalid_by_index_ != nullptr) delete globalid_by_index_;
    if (indegree_ != nullptr) delete indegree_;
    if (outdegree_ != nullptr) delete outdegree_;
    if (in_offset_ != nullptr) delete in_offset_;
    if (out_offset_ != nullptr) delete out_offset_;
    if (in_edges_ != nullptr) delete in_edges_;
    if (out_edges_ != nullptr) delete out_edges_;
  }

  std::unique_ptr<Serialized> Serialize(
      const common::TaskRunner& runner) override;

  void Deserialize(const common::TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override;

  void set_gid(GraphID gid) { gid_ = gid; }
  void set_num_vertices(const size_t val) { num_vertices_ = val; }
  void set_num_incoming_edges(const size_t val) { num_incoming_edges_ = val; }
  void set_num_outgoing_edges(const size_t val) { num_outgoing_edges_ = val; }
  void set_max_vid(const VertexID val) { max_vid_ = val; }
  void set_min_vid(const VertexID val) { min_vid_ = val; }

  GraphID get_gid() const { return gid_; }
  size_t get_num_vertices() const { return num_vertices_; }
  size_t get_num_incoming_edges() const { return num_incoming_edges_; }
  size_t get_num_outgoing_edges() const { return num_outgoing_edges_; }
  VertexID get_max_vid() const { return max_vid_; }
  VertexID get_min_vid() const { return min_vid_; }

  void SetGlobalIDbyIndex(VertexID* globalid_by_index) {
    globalid_by_index_ = globalid_by_index;
  }
  void SetInDegree(size_t* indegree) { indegree_ = indegree; }
  void SetOutDegree(size_t* outdegree) { outdegree_ = outdegree; }
  void SetInOffset(size_t* in_offset) { in_offset_ = in_offset; }
  void SetOutOffset(size_t* out_offset) { out_offset_ = out_offset; }
  void SetInEdges(VertexID* in_edges) { in_edges_ = in_edges; }
  void SetOutEdges(VertexID* out_edges) { out_edges_ = out_edges; }

  uint8_t* GetGraphBuffer() const { return buf_graph_; }
  VertexID* GetGlobalIDByIndex() const { return globalid_by_index_; }
  // VertexID* GetLocalIDByIndex() const { return localid_by_index_; }
  VertexID* GetLocalIDByGlobalID() const { return localid_by_globalid_; }
  VertexID* GetInEdges() const { return in_edges_; }
  VertexID* GetOutEdges() const { return out_edges_; }
  size_t* GetInDegree() const { return indegree_; }
  size_t* GetOutDegree() const { return outdegree_; }
  size_t* GetInOffset() const { return in_offset_; }
  size_t* GetOutOffset() const { return out_offset_; }

 private:
  void ParseSubgraphCSR(const std::list<OwnedBuffer>& buffer_list);

 private:
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_;

  GraphID gid_ = 0;
  size_t num_vertices_ = 0;
  size_t num_incoming_edges_ = 0;
  size_t num_outgoing_edges_ = 0;
  VertexID max_vid_ = 0;
  VertexID min_vid_ = 0;

  uint8_t* buf_graph_ = nullptr;

  // config. attributes to build the CSR.
  ImmutableCSRGraphConfig csr_config_;

  // serialized data in CSR format.
  VertexID* globalid_by_index_ = nullptr;
  VertexID* in_edges_ = nullptr;
  VertexID* out_edges_ = nullptr;
  size_t* indegree_ = nullptr;
  size_t* outdegree_ = nullptr;
  size_t* in_offset_ = nullptr;
  size_t* out_offset_ = nullptr;
  VertexID* localid_by_globalid_ = nullptr;
};
}  // namespace sics::graph::core::data_structures::graph

#endif  // CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_H_
