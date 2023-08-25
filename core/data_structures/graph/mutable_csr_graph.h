#ifndef GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_CSR_GRAPH_H_
#define GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_CSR_GRAPH_H_

#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph/serialized_mutable_csr_graph.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"
#include "util/pointer_cast.h"

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
  using SerializedMutableCSRGraph =
      data_structures::graph::SerializedMutableCSRGraph;

 public:
  using VertexData = TV;
  using EdgeData = TE;
  explicit MutableCSRGraph(const SubgraphMetadata& metadata)
      : metadata_(metadata) {}

  // Serializable interface override functions
  std::unique_ptr<Serialized> Serialize(
      const common::TaskRunner& runner) override {
    graph_buf_base_ = nullptr;
    vertex_id_by_local_index_ = nullptr;
    out_degree_base_ = nullptr;
    out_offset_base_ = nullptr;
    out_edges_base_ = nullptr;
    // write back
    return util::pointer_downcast<Serialized, SerializedMutableCSRGraph>(
        std::move(graph_serialized_));
  }

  void Deserialize(const common::TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override {
    graph_serialized_ =
        std::move(util::pointer_upcast<Serialized, SerializedMutableCSRGraph>(
            std::move(serialized)));

    graph_buf_base_ = graph_serialized_->GetCSRBuffer()->at(0).Get();

    // set the pointer to base address
    if (metadata_.num_incoming_edges == 0) {
      size_t offset = 0;
      vertex_id_by_local_index_ = (VertexID*)(graph_buf_base_ + offset);
      offset += sizeof(VertexID) * metadata_.num_vertices;
      out_degree_base_ = (VertexDegree*)(graph_buf_base_ + offset);
      offset += sizeof(VertexDegree) * metadata_.num_vertices;
      out_offset_base_ = (VertexOffset*)(graph_buf_base_ + offset);
      offset += sizeof(VertexOffset) * metadata_.num_vertices;
      assert(offset == serialized->PopNext().front().GetSize());
    } else {
      LOG_FATAL("Error in deserialize mutable csr graph");
    }

    out_edges_base_ =
        (VertexID*)(graph_serialized_->GetCSRBuffer()->at(1).Get());
  }

  // methods for sync data
  void SyncVertexData() {
    memcpy(vertex_data_base_, vertex_data_write_base_,
           sizeof(VertexData) * metadata_.num_vertices);
  }

  void MutateGraphEdge(common::TaskRunner* runner) {
    // TODO: use taskrunner to delete unused egdes
  }

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
  const SubgraphMetadata& metadata_;

  std::unique_ptr<data_structures::graph::SerializedMutableCSRGraph>
      graph_serialized_;

  // deserialized data pointer in CSR format

  uint8_t* graph_buf_base_;
  VertexID* vertex_id_by_local_index_;
  VertexDegree* out_degree_base_;
  VertexOffset* out_offset_base_;
  VertexID* out_edges_base_;

  VertexData* vertex_data_base_;
  common::Bitmap* vertex_src_or_dst_bitmap_;

  // used for mutable algorithm only;
  VertexDegree* out_degree_base_new_;
  VertexOffset* out_offset_base_new_;
  VertexID* out_edges_base_new_;
  VertexData* vertex_data_write_base_;

  std::string status_;
};

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_CSR_GRAPH_H_
