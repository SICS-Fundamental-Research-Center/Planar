#include "miniclean/data_structures/graphs/miniclean_graph.h"

#include "core/util/pointer_cast.h"

namespace sics::graph::miniclean::data_structures::graphs {

using Serialized = sics::graph::core::data_structures::Serialized;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;

std::unique_ptr<Serialized> MiniCleanGraph::Serialize(
    const TaskRunner& runner) {
  return sics::graph::core::util::pointer_downcast<Serialized,
                                                   SerializedMiniCleanGraph>(
      std::move(serialized_graph_));
}

void MiniCleanGraph::Deserialize(const TaskRunner& runner,
                                 std::unique_ptr<Serialized>&& serialized) {
  serialized_graph_ = sics::graph::core::util::pointer_upcast<
      Serialized, SerializedMiniCleanGraph>(std::move(serialized));

  const auto& miniclean_graph_buffers =
      serialized_graph_->GetMiniCleanGraphBuffers();

  auto iter = miniclean_graph_buffers.begin();

  // Parse subgraph CSR
  ParseSubgraphCSR((*iter++).front());

  // Parse is-in-graph bitmap
  ParseBitmapHandle((*iter++).front());

  // Parse vertex attribute
  if (metadata_.vattr_id_to_file_path.size() !=
      metadata_.vattr_id_to_vattr_type.size())
    LOG_FATAL("vattr_id_to_file_path.size() != vattr_id_to_vattr_type.size()");
  vattr_base_pointers_.resize(metadata_.vattr_id_to_file_path.size());
  vattr_types_.resize(metadata_.vattr_id_to_file_path.size());
  for (size_t i = 0; i < metadata_.vattr_id_to_file_path.size(); i++) {
    if ((*iter).empty()) {
      vattr_base_pointers_[i] = nullptr;
      vattr_types_[i] = metadata_.vattr_id_to_vattr_type[i];
      iter++;
      continue;
    }
    ParseVertexAttribute(i, (*iter++).front());
  }
}

void MiniCleanGraph::ParseSubgraphCSR(const OwnedBuffer& buffer) {
  // Fetch the OwnedBuffer object.
  graph_base_pointer_ = buffer.Get();

  // Compute the size of each buffer.
  size_t vertices_buffer_size = sizeof(VertexID) * metadata_.num_vertices;
  size_t offset_buffer_size = sizeof(EdgeIndex) * metadata_.num_vertices;
  size_t incoming_edges_buffer_size =
      sizeof(VertexID) * metadata_.num_incoming_edges;

  // Compute the start position of each buffer.
  size_t start_vidl_to_vidg = 0;
  size_t start_indegree = start_vidl_to_vidg + vertices_buffer_size;
  size_t start_outdegree = start_indegree + vertices_buffer_size;
  size_t start_in_offset = start_outdegree + vertices_buffer_size;
  size_t start_out_offset = start_in_offset + offset_buffer_size;
  size_t start_incoming_vidl = start_out_offset + offset_buffer_size;
  size_t start_outgoing_vidl = start_incoming_vidl + incoming_edges_buffer_size;

  // Initialize base pointers.
  vidl_to_vidg_base_pointer_ =
      reinterpret_cast<VertexID*>(graph_base_pointer_ + start_vidl_to_vidg);
  indegree_base_pointer_ =
      reinterpret_cast<VertexID*>(graph_base_pointer_ + start_indegree);
  outdegree_base_pointer_ =
      reinterpret_cast<VertexID*>(graph_base_pointer_ + start_outdegree);
  in_offset_base_pointer_ =
      reinterpret_cast<EdgeIndex*>(graph_base_pointer_ + start_in_offset);
  out_offset_base_pointer_ =
      reinterpret_cast<EdgeIndex*>(graph_base_pointer_ + start_out_offset);
  incoming_vidl_base_pointer_ =
      reinterpret_cast<VertexID*>(graph_base_pointer_ + start_incoming_vidl);
  outgoing_vidl_base_pointer_ =
      reinterpret_cast<VertexID*>(graph_base_pointer_ + start_outgoing_vidl);
}

void MiniCleanGraph::ParseBitmapHandle(const OwnedBuffer& buffer) {
  is_in_graph_bitmap_.Init(metadata_.num_vertices,
                           reinterpret_cast<uint64_t*>(buffer.Get()));
}

void MiniCleanGraph::ParseVertexAttribute(size_t vattr_id,
                                          const OwnedBuffer& buffer) {
  vattr_base_pointers_[vattr_id] = reinterpret_cast<uint8_t*>(buffer.Get());
  vattr_types_[vattr_id] = metadata_.vattr_id_to_vattr_type[vattr_id];
}

VertexLabel MiniCleanGraph::GetVertexLabel(VertexID vidl) const {
  for (VertexLabel i = 0; i < metadata_.vlabel_id_to_vidl_range.size(); i++) {
    if (vidl >= metadata_.vlabel_id_to_vidl_range[i].first &&
        vidl <= metadata_.vlabel_id_to_vidl_range[i].second) {
      return i;
    }
  }
  LOG_FATAL("Vertex label not found.");
}

}  // namespace sics::graph::miniclean::data_structures::graphs