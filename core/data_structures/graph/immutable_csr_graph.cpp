#include "data_structures/graph/immutable_csr_graph.h"

namespace sics::graph::core::data_structures::graph {

std::unique_ptr<Serialized> ImmutableCSRGraph::Serialize(
    const common::TaskRunner& runner) {
  return std::unique_ptr<Serialized>(
      static_cast<Serialized*>(serialized_.release()));
}

void ImmutableCSRGraph::Deserialize(const common::TaskRunner& runner,
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
    LOG_ERROR("More than one subgraph in a serialized immutable CSR graph.");
  }
}

void ImmutableCSRGraph::ParseSubgraphCSR(
    const std::vector<OwnedBuffer>& buffer_list) {
  // Fetch the OwnedBuffer object.
  buf_graph_base_pointer_ = buffer_list.front().Get();

  VertexID start_globalid = 0, start_indegree = 0, start_outdegree = 0,
           start_in_offset = 0, start_out_offset = 0, start_incoming_edges = 0,
           start_outgoing_edges = 0;

  size_t offset = 0,
         vertices_buffer_size = sizeof(VertexID) * metadata_.num_vertices,
         offset_buffer_size = sizeof(EdgeIndex) * metadata_.num_vertices,
         incoming_edges_buffer_size =
             sizeof(VertexID) * metadata_.num_incoming_edges;

  if (metadata_.num_outgoing_edges != 0 && metadata_.num_incoming_edges != 0) {
    start_globalid = offset;
    offset += vertices_buffer_size;
    start_indegree = offset;
    offset += vertices_buffer_size;
    start_outdegree = offset;
    offset += vertices_buffer_size;
    start_in_offset = offset;
    offset += offset_buffer_size;
    start_out_offset = offset;
    offset += offset_buffer_size;
    start_incoming_edges = offset;
    offset += incoming_edges_buffer_size;
    start_outgoing_edges = offset;
  } else if (metadata_.num_outgoing_edges != 0) {
    start_globalid = offset;
    offset += vertices_buffer_size;
    start_outdegree = offset;
    offset += vertices_buffer_size;
    start_out_offset = offset;
    offset += offset_buffer_size;
    start_outgoing_edges = offset;
  } else if (metadata_.num_incoming_edges != 0) {
    start_globalid = offset;
    offset += vertices_buffer_size;
    start_indegree = offset;
    offset += vertices_buffer_size;
    start_in_offset = offset;
    offset += offset_buffer_size;
    start_incoming_edges = offset;
  }

  SetGlobalIDBuffer(
      reinterpret_cast<VertexID*>(buf_graph_base_pointer_ + start_globalid));

  if (metadata_.num_incoming_edges != 0) {
    SetInDegreeBuffer(
        reinterpret_cast<VertexID*>(buf_graph_base_pointer_ + start_indegree));
    SetInOffsetBuffer(reinterpret_cast<EdgeIndex*>(buf_graph_base_pointer_ +
                                                   start_in_offset));
    SetIncomingEdgesBuffer(reinterpret_cast<VertexID*>(buf_graph_base_pointer_ +
                                                       start_incoming_edges));
  }
  if (metadata_.num_outgoing_edges != 0) {
    SetOutDegreeBuffer(
        reinterpret_cast<VertexID*>(buf_graph_base_pointer_ + start_outdegree));
    SetOutOffsetBuffer(reinterpret_cast<EdgeIndex*>(buf_graph_base_pointer_ +
                                                    start_out_offset));
    SetOutgoingEdgesBuffer(reinterpret_cast<VertexID*>(buf_graph_base_pointer_ +
                                                       start_outgoing_edges));
  }
  gid_ = metadata_.gid;
  num_vertices_ = metadata_.num_vertices;
  num_incoming_edges_ = metadata_.num_incoming_edges;
  num_outgoing_edges_ = metadata_.num_outgoing_edges;
  max_vid_ = metadata_.max_vid;
  min_vid_ = metadata_.min_vid;
}

}  // namespace sics::graph::core::data_structures::graph
