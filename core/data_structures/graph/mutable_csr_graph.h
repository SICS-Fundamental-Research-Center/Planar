#ifndef GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_CSR_GRAPH_H_
#define GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_CSR_GRAPH_H_

#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"

namespace sics::graph::core::data_structures::graph {

// TV : type of vertexData; TE : type of EdgeData
template <typename TV, typename TE>
class MutableCSRGraph : public Serializable {
  using GraphID = common::GraphID;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexDegree = uint32_t;
  using VertexOffset = uint32_t;

 public:
  using VertexData = TV;
  using EdgeData = TE;
  explicit MutableCSRGraph(const SubgraphMetadata& metadata)
      : metadata_(metadata) {}

  // Serializable interface override functions
  std::unique_ptr<Serialized> Serialize(
      const common::TaskRunner& runner) override {
    // TODO: transfer the data from MutableCSRgraph to Serialized structure for
    // write back
  }

  void Deserialize(const common::TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override {
    // TODO: transfer the serialized data to the MutableCSRgraph
  }

  // methods for sync data
  void SyncVertexData() {}

  void MutateGraphEdge() {}

  // methods for vertex info

  common::VertexCount GetVertexNums() const { return metadata_.num_vertices; }
  size_t GetOutEdgeNums() const { return metadata_.num_outgoing_edges; }

  VertexID GetVertexIDByIndex(VertexIndex index) const {
    return vertex_id_by_local_index_[index];
  }

  VertexDegree GetOutDegreeByIndex(VertexIndex index) const {
    return out_degree_base_[index];
  }

  VertexOffset GetOutOffsetByIndex(VertexIndex index) const {
    return out_offset_base_[index];
  }

  VertexID GetOneOutEdge(VertexIndex index, VertexOffset out_offset) const {
    return out_offset_base_[index] + out_offset;
  }

  VertexID* GetOutEdges(VertexIndex index) const {
    return out_offset_base_ + index;
  }

  VertexData* GetVertxDataByIndex(VertexIndex index) const {
    return vertex_data_base_ + index;
  }

  void set_status(const std::string& new_status) { status_ = new_status; }

 private:
  SubgraphMetadata metadata_;

  // deserialized data in CSR format
  uint8_t* graph_buf_base_;
  VertexID* vertex_id_by_local_index_;
  VertexDegree* out_degree_base_;
  VertexOffset* out_offset_base_;
  VertexID* out_edges_base_;

  VertexData* vertex_data_base_;
  common::Bitmap* vertex_src_or_dst_bitmap_;

  std::string status_;
};

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_CSR_GRAPH_H_
