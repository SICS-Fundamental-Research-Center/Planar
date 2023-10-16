#include "data_structures/graph/immutable_csr_graph.h"

namespace xyz::graph::core::data_structures::graph {

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

  SetGlobalIDBuffer(reinterpret_cast<VertexID*>(
      reinterpret_cast<char*>(buf_graph_base_pointer_)));

  if (metadata_.num_incoming_edges != 0 && metadata_.num_outgoing_edges != 0) {
    SetInDegreeBuffer(
        reinterpret_cast<VertexID*>(globalid_by_localid_base_pointer_) +
        metadata_.num_vertices);
    SetOutDegreeBuffer(reinterpret_cast<VertexID*>(indegree_base_pointer_ +
                                                   metadata_.num_vertices));
    SetInOffsetBuffer(reinterpret_cast<EdgeIndex*>(outdegree_base_pointer_ +
                                                   metadata_.num_vertices));
    SetOutOffsetBuffer(reinterpret_cast<EdgeIndex*>(in_offset_base_pointer_ +
                                                    metadata_.num_vertices));
    SetIncomingEdgesBuffer(reinterpret_cast<VertexID*>(
        out_offset_base_pointer_ + metadata_.num_vertices));
    SetOutgoingEdgesBuffer(reinterpret_cast<VertexID*>(
        incoming_edges_base_pointer_ + metadata_.num_incoming_edges));

  } else if (metadata_.num_incoming_edges != 0) {
    SetInDegreeBuffer(
        reinterpret_cast<VertexID*>(globalid_by_localid_base_pointer_) +
        metadata_.num_vertices);
    SetInOffsetBuffer(reinterpret_cast<EdgeIndex*>(indegree_base_pointer_ +
                                                   metadata_.num_vertices));
    SetIncomingEdgesBuffer(reinterpret_cast<VertexID*>(in_offset_base_pointer_ +
                                                       metadata_.num_vertices));

  } else if (metadata_.num_outgoing_edges != 0) {
    SetOutDegreeBuffer(reinterpret_cast<VertexID*>(
        globalid_by_localid_base_pointer_ + metadata_.num_vertices));
    SetOutOffsetBuffer(reinterpret_cast<EdgeIndex*>(outdegree_base_pointer_ +
                                                    metadata_.num_vertices));
    SetOutgoingEdgesBuffer(reinterpret_cast<VertexID*>(
        out_offset_base_pointer_ + metadata_.num_vertices));
  }

  gid_ = metadata_.gid;
  num_vertices_ = metadata_.num_vertices;
  num_incoming_edges_ = metadata_.num_incoming_edges;
  num_outgoing_edges_ = metadata_.num_outgoing_edges;
  max_vid_ = metadata_.max_vid;
  min_vid_ = metadata_.min_vid;
}

}  // namespace xyz::graph::core::data_structures::graph
