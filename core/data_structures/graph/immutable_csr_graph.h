#ifndef CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_H_
#define CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_H_

#include <cmath>
#include <list>
#include <memory>
#include <utility>

#include "common/types.h"
#include "data_structures/graph/immutable_csr_graph_config.h"
#include "data_structures/graph/serialized_immutable_csr_graph.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"
#include "util/logging.h"

namespace sics::graph::core::data_structures::graph {

struct ImmutableCSRVertex {
 private:
  using VertexID = sics::graph::core::common::VertexID;
  using SubgraphMetadata = sics::graph::core::data_structures::SubgraphMetadata;

 public:
  VertexID vid;
  VertexID indegree;
  VertexID outdegree;
  VertexID* in_edges;
  VertexID* out_edges;
};

class ImmutableCSRGraph : public Serializable {
 private:
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;

 public:
  ImmutableCSRGraph(const GraphID gid) : Serializable(), gid_(gid) {}
  ImmutableCSRGraph(const GraphID gid, ImmutableCSRGraphConfig&& csr_config)
      : Serializable(), gid_(gid), csr_config_(std::move(csr_config)) {}
  
  explicit ImmutableCSRGraph(SubgraphMetadata& metadata)
      : Serializable(), metadata_(metadata) {}


  virtual std::unique_ptr<Serialized> Serialize(
      const common::TaskRunner& runner) override;

  virtual void Deserialize(const common::TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override;

  GraphID get_gid() const { return gid_; }
  void set_gid(GraphID gid) { gid_ = gid; }
  void set_num_vertices(VertexID val) { num_vertices_ = val; }
  void set_num_incoming_edges(VertexID val) { num_incoming_edges_ = val; }
  void set_num_outgoing_edges(VertexID val) { num_outgoing_edges_ = val; }
  void set_max_vid(VertexID val) { max_vid_ = val; }
  void set_min_vid(VertexID val) { min_vid_ = val; }

  GraphID get_gid() const { return gid_; }
  VertexID get_num_vertices() const { return num_vertices_; }
  VertexID get_num_incoming_edges() const { return num_incoming_edges_; }
  VertexID get_num_outgoing_edges() const { return num_outgoing_edges_; }
  VertexID get_max_vid() const { return max_vid_; }
  VertexID get_min_vid() const { return min_vid_; }

  void SetGlobalID(VertexID* globalid_by_index) {
    globalid_by_index_ = globalid_by_index;
  }
  void SetInDegree(VertexID* indegree) { indegree_ = indegree; }
  void SetOutDegree(VertexID* outdegree) { outdegree_ = outdegree; }
  void SetInOffset(VertexID* in_offset) { in_offset_ = in_offset; }
  void SetOutOffset(VertexID* out_offset) { out_offset_ = out_offset; }
  void SetInEdges(VertexID* in_edges) { in_edges_ = in_edges; }
  void SetOutEdges(VertexID* out_edges) { out_edges_ = out_edges; }

  uint8_t* GetGraphBuffer() const { return buf_graph_; }
  VertexID* GetGlobalIDBuffer() const { return globalid_by_index_; }
  VertexID* GetInEdges() const { return in_edges_; }
  VertexID* GetOutEdges() const { return out_edges_; }
  VertexID* GetInDegree() const { return indegree_; }
  VertexID* GetOutDegree() const { return outdegree_; }
  VertexID* GetInOffset() const { return in_offset_; }
  VertexID* GetOutOffset() const { return out_offset_; }

 protected:
  void ParseSubgraphCSR(const std::list<OwnedBuffer>& buffer_list);

  std::unique_ptr<SerializedImmutableCSRGraph> serialized_;
  GraphID gid_ = 0;
  VertexID num_vertices_ = 0;
  VertexID num_incoming_edges_ = 0;
  VertexID num_outgoing_edges_ = 0;
  VertexID max_vid_ = 0;
  VertexID min_vid_ = 0;

  uint8_t* buf_graph_;
  SubgraphMetadata metadata_;

  // serialized data in CSR format.
  VertexID* globalid_by_index_;
  VertexID* in_edges_;
  VertexID* out_edges_;
  VertexID* indegree_;
  VertexID* outdegree_;
  VertexID* in_offset_;
  VertexID* out_offset_;
  VertexID* localid_by_globalid_;

 private:
  // config. attributes to build the CSR.
  ImmutableCSRGraphConfig csr_config_;

};

}  // namespace sics::graph::core::data_structures::graph

#endif  // CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_H_
