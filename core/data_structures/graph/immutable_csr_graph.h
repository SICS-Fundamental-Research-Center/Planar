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

 public:
  VertexID vid;
  VertexID indegree = 0;
  VertexID outdegree = 0;
  VertexID* incoming_edges;
  VertexID* outgoing_edges;
};

class ImmutableCSRGraph : public Serializable {
 private:
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;
  using VertexLabel = sics::graph::core::common::VertexLabel;
  using EdgeIndex = sics::graph::core::common::EdgeIndex;

 public:
  explicit ImmutableCSRGraph(SubgraphMetadata metadata)
      : Serializable(), metadata_(metadata) {}

  explicit ImmutableCSRGraph(GraphID gid) : Serializable(), gid_(gid) {}

  ImmutableCSRGraph() : Serializable() {}

  std::unique_ptr<Serialized> Serialize(
      const common::TaskRunner& runner) override;

  void Deserialize(const common::TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override;

  void ShowGraph(VertexID display_num = 0) {
    LOG_INFO("### GID: ", gid_, ",  num_vertices: ", num_vertices_,
             ", num_incoming_edges: ", num_incoming_edges_,
             ", num_outgoing_edges: ", num_outgoing_edges_, " Show top ",
             display_num, " ###");
    for (VertexID i = 0; i < display_num; i++) {
      if (i >= num_vertices_) break;
      auto u = GetVertexByLocalID(i);

      std::stringstream ss;
      ss << "  ===vid: " << u.vid << ", indegree: " << u.indegree
         << ", outdegree: " << u.outdegree << "===" << std::endl;
      if (u.indegree != 0) {
        ss << "    Incoming edges: ";
        for (VertexID i = 0; i < u.indegree; i++)
          ss << u.incoming_edges[i] << ",";
        ss << std::endl << std::endl;
      }
      if (u.outdegree != 0) {
        ss << "    Outgoing edges: ";
        for (VertexID i = 0; i < u.outdegree; i++)
          ss << u.outgoing_edges[i] << ",";
        ss << std::endl << std::endl;
      }
      ss << "****************************************" << std::endl;
      std::string s = ss.str();

      LOG_INFO(s);
    }
  }

  void set_gid(GraphID gid) { gid_ = gid; }
  void set_num_vertices(VertexID val) { num_vertices_ = val; }
  void set_num_incoming_edges(EdgeIndex val) { num_incoming_edges_ = val; }
  void set_num_outgoing_edges(EdgeIndex val) { num_outgoing_edges_ = val; }
  void set_max_vid(const VertexID val) { max_vid_ = val; }
  void set_min_vid(const VertexID val) { min_vid_ = val; }

  GraphID get_gid() const { return gid_; }
  VertexID get_num_vertices() const { return num_vertices_; }
  EdgeIndex get_num_incoming_edges() const { return num_incoming_edges_; }
  EdgeIndex get_num_outgoing_edges() const { return num_outgoing_edges_; }
  VertexID get_max_vid() const { return max_vid_; }
  VertexID get_min_vid() const { return min_vid_; }

  void SetGraphBuffer(uint8_t* buffer) { buf_graph_base_pointer_ = buffer; }

  void SetGlobalIDBuffer(VertexID* buffer) {
    globalid_by_localid_base_pointer_ = buffer;
  }
  void SetInDegreeBuffer(VertexID* buffer) { indegree_base_pointer_ = buffer; }
  void SetOutDegreeBuffer(VertexID* buffer) {
    outdegree_base_pointer_ = buffer;
  }
  void SetInOffsetBuffer(EdgeIndex* buffer) {
    in_offset_base_pointer_ = buffer;
  }
  void SetOutOffsetBuffer(EdgeIndex* buffer) {
    out_offset_base_pointer_ = buffer;
  }
  void SetIncomingEdgesBuffer(VertexID* buffer) {
    incoming_edges_base_pointer_ = buffer;
  }
  void SetOutgoingEdgesBuffer(VertexID* buffer) {
    outgoing_edges_base_pointer_ = buffer;
  }
  void SetVertexLabelBuffer(VertexLabel* buffer) {
    vertex_label_base_pointer_ = buffer;
  }

  uint8_t* GetGraphBuffer() { return buf_graph_base_pointer_; }
  VertexID* GetGloablIDBasePointer() const {
    return globalid_by_localid_base_pointer_;
  }
  VertexID* GetInDegreeBasePointer() const { return indegree_base_pointer_; }
  VertexID* GetOutDegreeBasePointer() const { return outdegree_base_pointer_; }
  EdgeIndex* GetInOffsetBasePointer() const { return in_offset_base_pointer_; }
  EdgeIndex* GetOutOffsetBasePointer() const {
    return out_offset_base_pointer_;
  }
  VertexID* GetIncomingEdgesBasePointer() const {
    return incoming_edges_base_pointer_;
  }
  VertexID* GetOutgoingEdgesBasePointer() const {
    return outgoing_edges_base_pointer_;
  }

  VertexID GetGlobalIDByLocalID(VertexID i) const {
    return globalid_by_localid_base_pointer_[i];
  }
  VertexID GetInOffsetByLocalID(VertexID i) const {
    return in_offset_base_pointer_[i];
  }
  VertexID GetOutOffsetByLocalID(VertexID i) const {
    return out_offset_base_pointer_[i];
  }
  VertexID* GetIncomingEdgesByLocalID(VertexID i) const {
    return incoming_edges_base_pointer_ + in_offset_base_pointer_[i];
  }
  VertexID* GetOutgoingEdgesByLocalID(VertexID i) const {
    return outgoing_edges_base_pointer_ + out_offset_base_pointer_[i];
  }
  VertexID GetInDegreeByLocalID(VertexID i) const {
    return indegree_base_pointer_[i];
  }
  VertexID GetOutDegreeByLocalID(VertexID i) const {
    return outdegree_base_pointer_[i];
  }

  ImmutableCSRVertex GetVertexByLocalID(VertexID i) const {
    ImmutableCSRVertex v;
    v.vid = GetGlobalIDByLocalID(i);
    if (num_incoming_edges_ > 0) {
      v.indegree = GetInDegreeByLocalID(i);
      v.incoming_edges = incoming_edges_base_pointer_ + GetInOffsetByLocalID(i);
    }
    if (num_outgoing_edges_ > 0) {
      v.outdegree = GetOutDegreeByLocalID(i);
      v.outgoing_edges =
          outgoing_edges_base_pointer_ + GetOutOffsetByLocalID(i);
    }
    return v;
  }

 private:
  void ParseSubgraphCSR(const std::vector<OwnedBuffer>& buffer_list);

 protected:
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_;

  GraphID gid_ = 0;
  VertexID num_vertices_ = 0;
  EdgeIndex num_incoming_edges_ = 0;
  EdgeIndex num_outgoing_edges_ = 0;
  VertexID max_vid_ = 0;
  VertexID min_vid_ = 0;

  // config. Metadata to build the CSR.
  SubgraphMetadata metadata_;

  // serialized data in CSR format.
  uint8_t* buf_graph_base_pointer_;
  VertexID* globalid_by_localid_base_pointer_;
  VertexID* incoming_edges_base_pointer_;
  VertexID* outgoing_edges_base_pointer_;
  VertexID* indegree_base_pointer_;
  VertexID* outdegree_base_pointer_;
  EdgeIndex* in_offset_base_pointer_;
  EdgeIndex* out_offset_base_pointer_;
  VertexLabel* vertex_label_base_pointer_;
};

}  // namespace sics::graph::core::data_structures::graph

#endif  // CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_H_