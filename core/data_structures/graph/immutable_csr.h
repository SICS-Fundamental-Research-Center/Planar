#ifndef CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_H_
#define CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_H_

#include "common/types.h"
#include "data_structures/graph/immutable_csr_graph_config.h"
#include "data_structures/graph/serialized_immutable_csr_graph.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"
#include "util/logging.h"
#include <cmath>
#include <list>
#include <memory>
#include <utility>

namespace sics::graph::core::data_structures::graph {

struct CSRVertex {
 private:
  using VertexID = sics::graph::core::common::VertexID;
  using SubgraphMetadata = sics::graph::core::data_structures::SubgraphMetadata;

 public:
  void ShowVertex() {
    std::cout << "  ===vid: " << vid << ", indegree: " << indegree
              << ", outdegree: " << outdegree << "===" << std::endl;
    if (indegree != 0) {
      std::cout << "    Incoming edges: ";
      for (VertexID i = 0; i < indegree; i++)
        std::cout << incoming_edges[i] << ",";
      std::cout << std::endl << std::endl;
    }
    if (outdegree != 0) {
      std::cout << "    Outgoing edges: ";
      for (VertexID i = 0; i < outdegree; i++)
        std::cout << outgoing_edges[i] << ",";
      std::cout << std::endl << std::endl;
    }
    std::cout << "****************************************" << std::endl;
  }

  VertexID vid;
  VertexID indegree = 0;
  VertexID outdegree = 0;
  VertexID* incoming_edges = nullptr;
  VertexID* outgoing_edges = nullptr;
};

class ImmutableCSR : public Serializable {
 private:
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;

 public:
  explicit ImmutableCSR(SubgraphMetadata metadata)
      : Serializable(), metadata_(metadata) {}

  std::unique_ptr<Serialized> Serialize(
      const common::TaskRunner& runner) override;

  void Deserialize(const common::TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override;

  void ShowGraph(VertexID display_num = 0) {
    LOG_INFO("### GID: ", gid_, ",  num_vertices: ", num_vertices_,
             ", num_incoming_edges: ", num_incoming_edges_,
             ", num_outgoing_edges: ", num_outgoing_edges_, " ###");
    for (VertexID i = 0; i < display_num; i++) {
      if (i >= num_vertices_) break;
      auto u = GetVertexByIndex(i);
      u.ShowVertex();
    }
  }

  inline void set_gid(GraphID gid) { gid_ = gid; }
  inline void set_num_vertices(VertexID val) { num_vertices_ = val; }
  inline void set_num_incoming_edges(VertexID val) {
    num_incoming_edges_ = val;
  }
  inline void set_num_outgoing_edges(VertexID val) {
    num_outgoing_edges_ = val;
  }
  inline void set_max_vid(const VertexID val) { max_vid_ = val; }
  inline void set_min_vid(const VertexID val) { min_vid_ = val; }

  inline GraphID get_gid() const { return gid_; }
  inline VertexID get_num_vertices() const { return num_vertices_; }
  inline VertexID get_num_incoming_edges() const { return num_incoming_edges_; }
  inline VertexID get_num_outgoing_edges() const { return num_outgoing_edges_; }
  inline VertexID get_max_vid() const { return max_vid_; }
  inline VertexID get_min_vid() const { return min_vid_; }

  void SetGlobalIDbyIndexBuffer(VertexID* buffer) {
    globalid_by_index_base_pointer_ = buffer;
  }
  void SetInDegreeBuffer(VertexID* buffer) { indegree_base_pointer_ = buffer; }
  void SetOutDegreeBuffer(VertexID* buffer) {
    outdegree_base_pointer_ = buffer;
  }
  void SetInOffsetBuffer(VertexID* buffer) { in_offset_base_pointer_ = buffer; }
  void SetOutOffsetBuffer(VertexID* buffer) {
    out_offset_base_pointer_ = buffer;
  }
  void SetInEdgesBuffer(VertexID* buffer) {
    incoming_edges_base_pointer_ = buffer;
  }
  void SetOutEdgesBuffer(VertexID* buffer) {
    outgoing_edges_base_pointer_ = buffer;
  }

  uint8_t* GetGraphBuffer() const { return buf_graph_base_pointer_; }

  VertexID GetGlobalIDByIndex(VertexID index) const {
    return globalid_by_index_base_pointer_[index];
  }

  inline VertexID GetInOffsetByIndex(VertexID index) const {
    return in_offset_base_pointer_[index];
  }

  inline VertexID GetOutOffsetByIndex(VertexID index) const {
    return out_offset_base_pointer_[index];
  }

  inline VertexID* GetIncomingEdgesByIndex(VertexID index) {
    return incoming_edges_base_pointer_ + index;
  }

  inline VertexID* GetOutgoingEdgesByIndex(VertexID index) {
    return outgoing_edges_base_pointer_ + index;
  }

  inline VertexID GetInDegreeByIndex(VertexID index) const {
    return indegree_base_pointer_[index];
  }

  inline VertexID GetOutDegreeByIndex(VertexID index) const {
    return outdegree_base_pointer_[index];
  }

  CSRVertex GetVertexByIndex(VertexID index) const {
    CSRVertex v;
    v.vid = GetGlobalIDByIndex(index);
    if (num_incoming_edges_ > 0) {
      v.indegree = GetInDegreeByIndex(index);
      v.incoming_edges =
          incoming_edges_base_pointer_ + GetInOffsetByIndex(index);
    }
    if (num_outgoing_edges_ > 0) {
      v.outdegree = GetOutDegreeByIndex(index);
      v.outgoing_edges =
          outgoing_edges_base_pointer_ + GetOutOffsetByIndex(index);
    }
    return v;
  }

 private:
  void ParseSubgraphCSR(const std::list<OwnedBuffer>& buffer_list);

 private:
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_;

  GraphID gid_ = 0;
  VertexID num_vertices_ = 0;
  VertexID num_incoming_edges_ = 0;
  VertexID num_outgoing_edges_ = 0;
  VertexID max_vid_ = 0;
  VertexID min_vid_ = 0;

  // config. attributes to build the CSR.
  SubgraphMetadata metadata_;

  // serialized data in CSR format.
  uint8_t* buf_graph_base_pointer_ = nullptr;
  VertexID* globalid_by_index_base_pointer_ = nullptr;
  VertexID* incoming_edges_base_pointer_ = nullptr;
  VertexID* outgoing_edges_base_pointer_ = nullptr;
  VertexID* indegree_base_pointer_ = nullptr;
  VertexID* outdegree_base_pointer_ = nullptr;
  VertexID* in_offset_base_pointer_ = nullptr;
  VertexID* out_offset_base_pointer_ = nullptr;
};

}  // namespace sics::graph::core::data_structures::graph

#endif  // CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_H_
