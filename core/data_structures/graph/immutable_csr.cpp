#include "immutable_csr.h"

namespace sics::graph::core::data_structures::graph {

std::unique_ptr<Serialized> ImmutableCSR::Serialize(
    const common::TaskRunner& runner) {
  return std::unique_ptr<Serialized>(
      static_cast<Serialized*>(serialized_.release()));
}

void ImmutableCSR::Deserialize(const common::TaskRunner& runner,
                               std::unique_ptr<Serialized>&& serialized) {
  LOG_INFO("ImmutableCSR::Deserialize");
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

void ImmutableCSR::ParseSubgraphCSR(const std::list<OwnedBuffer>& buffer_list) {
  // Fetch the OwnedBuffer object.
  buf_graph_base_pointer_ = buffer_list.front().Get();

  VertexID start_globalid = 0, start_indegree = 0, start_outdegree = 0,
           start_in_offset = 0, start_out_offset = 0, start_incoming_edges = 0,
           start_outgoing_edges = 0;

  size_t offset = 0,
         vertices_buffer_size = sizeof(VertexID) * metadata_.num_vertices,
         incoming_edges_buffer_size =
             sizeof(VertexID) * metadata_.num_outgoing_edges,
         outgoing_edges_buffer_size =
             sizeof(VertexID) * metadata_.num_outgoing_edges;
  if (metadata_.num_incoming_edges != 0) {
    start_indegree = offset + vertices_buffer_size;
    offset = start_indegree;
    LOG_INFO(offset);
  }
  if (metadata_.num_outgoing_edges != 0) {
    start_outdegree = offset + vertices_buffer_size;
    offset = start_outdegree;
    LOG_INFO(offset);
  }
  if (metadata_.num_incoming_edges != 0) {
    start_in_offset = offset + vertices_buffer_size;
    offset = start_in_offset;
    LOG_INFO(offset);
  }
  if (metadata_.num_outgoing_edges != 0) {
    start_out_offset = offset + vertices_buffer_size;
    offset = start_out_offset;
    LOG_INFO(offset);
  }
  if (metadata_.num_incoming_edges != 0) {
    start_incoming_edges = offset + vertices_buffer_size;
    offset = start_incoming_edges;
    LOG_INFO(offset);
  }
  if (metadata_.num_outgoing_edges != 0) {
    start_outgoing_edges = offset + vertices_buffer_size;
    LOG_INFO(offset);
  }

  LOG_INFO("start_globalid: ", start_globalid);
  LOG_INFO("start_indegree: ", start_indegree);
  LOG_INFO("start_outdegree: ", start_outdegree);
  LOG_INFO("start_in_offset: ", start_in_offset);
  LOG_INFO("start_out_offset: ", start_out_offset);
  LOG_INFO("start_incoming_edges: ", start_incoming_edges);
  LOG_INFO("start_outgoing_edges: ", start_outgoing_edges);

  SetGlobalIDbyIndexBuffer(
      reinterpret_cast<VertexID*>(buf_graph_base_pointer_ + start_globalid));

  if (metadata_.num_incoming_edges != 0) {
    SetInDegreeBuffer(
        reinterpret_cast<VertexID*>(buf_graph_base_pointer_ + start_indegree));
    SetInOffsetBuffer(
        reinterpret_cast<VertexID*>(buf_graph_base_pointer_ + start_in_offset));
    SetInEdgesBuffer(reinterpret_cast<VertexID*>(buf_graph_base_pointer_ +
                                                 start_incoming_edges));
  }
  if (metadata_.num_outgoing_edges != 0) {
    SetOutDegreeBuffer(
        reinterpret_cast<VertexID*>(buf_graph_base_pointer_ + start_outdegree));
    SetOutOffsetBuffer(reinterpret_cast<VertexID*>(buf_graph_base_pointer_ +
                                                   start_out_offset));
    SetOutEdgesBuffer(reinterpret_cast<VertexID*>(buf_graph_base_pointer_ +
                                                  start_outgoing_edges));
  }
  num_vertices_ = metadata_.num_vertices;
  num_incoming_edges_ = metadata_.num_incoming_edges;
  num_outgoing_edges_ = metadata_.num_outgoing_edges;
  max_vid_ = metadata_.max_vid;
  min_vid_ = metadata_.min_vid;
  LOG_INFO(num_incoming_edges_, "  ", num_outgoing_edges_,
           " max_vid: ", max_vid_, " min_vid:  ", min_vid_);
  for (size_t i = 0; i < num_vertices_; i++) {
    LOG_INFO(GetGlobalIDByIndex(i), " / ", num_vertices_, " ",
             GetOutOffsetByIndex(i), " AAAA", GetOutDegreeByIndex(i));
  }
  for (size_t i = 0; i < num_vertices_; i++) {
    LOG_INFO("id: ", ((VertexID*)buf_graph_base_pointer_)[i], " outdegree: ",
             ((VertexID*)buf_graph_base_pointer_)[i + num_vertices_],
             " outoffset: ",
             ((VertexID*)buf_graph_base_pointer_)[i + 2 * num_vertices_]);
  }
  LOG_INFO("!!!!!!!!!", num_vertices_);
}

}  // namespace sics::graph::core::data_structures::graph
