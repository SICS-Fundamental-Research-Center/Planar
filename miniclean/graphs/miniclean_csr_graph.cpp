#include "miniclean/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::graphs {
using Serialized = sics::graph::core::data_structures::Serialized;

std::unique_ptr<Serialized> MiniCleanCSRGraph::Serialize(
    const TaskRunner& runner) {
  return nullptr;
}

void MiniCleanCSRGraph::Deserialize(const TaskRunner& runner,
                                    std::unique_ptr<Serialized>&& serialized) {
  auto new_serialized = std::unique_ptr<SerializedImmutableCSRGraph>(
      static_cast<SerializedImmutableCSRGraph*>(serialized.release()));
  if (new_serialized) {
    serialized_.swap(new_serialized);
  } else {
    LOG_ERROR("Failed to cast Serialized to SerializedImmutableCSRGraph.");
  }

  auto& csr_buffer = serialized_->GetCSRBuffer();
  ParseSubgraphCSR(csr_buffer.front());
  auto iter = csr_buffer.begin();
  if (iter != csr_buffer.end()) {
    ParseSubgraphCSR(*iter++);
  }
  if (iter != csr_buffer.end()) {
    // Parse in edge label
    ParseInedgeLabel(*iter++);
  }
  if (iter != csr_buffer.end()) {
    // Parse out edge label
    ParseOutedgeLabel(*iter++);
  }
  if (iter != csr_buffer.end()) {
    // Parse vertex label
    ParseVertexLabel(*iter++);
  }
}

void MiniCleanCSRGraph::ParseSubgraphCSR(
    const std::list<OwnedBuffer>& buffer_list) {
  // Fetch the OwnedBuffer object.
  uint8_t* buf_graph_base_pointer = buffer_list.front().Get();
  SetGraphBuffer(buf_graph_base_pointer);

  // Initialize metadata.
  gid_ = metadata_.gid;
  num_vertices_ = metadata_.num_vertices;
  num_incoming_edges_ = metadata_.num_incoming_edges;
  num_outgoing_edges_ = metadata_.num_outgoing_edges;
  max_vid_ = metadata_.max_vid;
  min_vid_ = metadata_.min_vid;

  VertexID start_globalid = 0, start_indegree = 0, start_outdegree = 0,
           start_in_offset = 0, start_out_offset = 0, start_incoming_edges = 0,
           start_outgoing_edges = 0;

  size_t vertices_buffer_size = sizeof(VertexID) * num_vertices_,
         incoming_edges_buffer_size = sizeof(VertexID) * num_incoming_edges_;

  start_globalid = 0;
  start_indegree = start_globalid + vertices_buffer_size;
  start_outdegree = start_indegree + vertices_buffer_size;
  start_in_offset = start_outdegree + vertices_buffer_size;
  start_out_offset = start_in_offset + vertices_buffer_size;
  start_incoming_edges = start_out_offset + vertices_buffer_size;
  start_outgoing_edges = start_incoming_edges + incoming_edges_buffer_size;

  // Initialize base pointers.
  SetGlobalIDBuffer(
      reinterpret_cast<VertexID*>(buf_graph_base_pointer + start_globalid));
  SetInDegreeBuffer(
      reinterpret_cast<VertexID*>(buf_graph_base_pointer + start_indegree));
  SetInOffsetBuffer(
      reinterpret_cast<VertexID*>(buf_graph_base_pointer + start_in_offset));
  SetIncomingEdgesBuffer(reinterpret_cast<VertexID*>(buf_graph_base_pointer +
                                                     start_incoming_edges));
  SetOutDegreeBuffer(
      reinterpret_cast<VertexID*>(buf_graph_base_pointer + start_outdegree));
  SetOutOffsetBuffer(
      reinterpret_cast<VertexID*>(buf_graph_base_pointer + start_out_offset));
  SetOutgoingEdgesBuffer(reinterpret_cast<VertexID*>(buf_graph_base_pointer +
                                                     start_outgoing_edges));
}

void MiniCleanCSRGraph::ParseVertexLabel(
    const std::list<OwnedBuffer>& buffer_list) {
  vertex_label_base_pointer_ =
      reinterpret_cast<VertexID*>(buffer_list.front().Get());
}
void MiniCleanCSRGraph::ParseInedgeLabel(
    const std::list<OwnedBuffer>& buffer_list) {
  in_edge_label_base_pointer_ =
      reinterpret_cast<VertexID*>(buffer_list.front().Get());
}
void MiniCleanCSRGraph::ParseOutedgeLabel(
    const std::list<OwnedBuffer>& buffer_list) {
  out_edge_label_base_pointer_ =
      reinterpret_cast<VertexID*>(buffer_list.front().Get());
}

}  // namespace sics::graph::miniclean::graphs