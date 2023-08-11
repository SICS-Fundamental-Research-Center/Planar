#include "immutable_csr_graph.h"

namespace sics::graph::core::data_structures::graph {

std::unique_ptr<Serialized> ImmutableCSRGraph::Serialize(
    const common::TaskRunner& runner) {
  return std::unique_ptr<Serialized>(
      static_cast<Serialized*>(serialized_.release()));
}

void ImmutableCSRGraph::Deserialize(const common::TaskRunner& runner,
                                    std::unique_ptr<Serialized>&& serialized) {
  // TODO(bwc): Submit to the task runner.
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
    const std::list<OwnedBuffer>& buffer_list) {
  // Fetch the OwnedBuffer object.
  buf_graph_ = buffer_list.front().Get();

  VertexID aligned_max_vertex = ((csr_config_.max_vertex >> 6) << 6 ) * 64;

  VertexID size_globalid = sizeof(VertexID) * csr_config_.num_vertex;
  VertexID size_indegree = sizeof(VertexID) * csr_config_.num_vertex;
  VertexID size_outdegree = sizeof(VertexID) * csr_config_.num_vertex;
  VertexID size_in_offset = sizeof(VertexID) * csr_config_.num_vertex;
  VertexID size_out_offset = sizeof(VertexID) * csr_config_.num_vertex;
  VertexID size_in_edges = sizeof(VertexID) * csr_config_.sum_in_edges;
  VertexID size_out_edges = sizeof(VertexID) * csr_config_.sum_out_edges;
  VertexID size_localid_by_globalid = sizeof(VertexID) * aligned_max_vertex;

  VertexID start_globalid = 0;
  VertexID start_indegree = start_globalid + size_globalid;
  VertexID start_outdegree = start_indegree + size_indegree;
  VertexID start_in_offset = start_outdegree + size_outdegree;
  VertexID start_out_offset = start_in_offset + size_in_offset;
  VertexID start_in_edges = start_out_offset + size_out_offset;
  VertexID start_out_edges = start_in_edges + size_in_edges;
  VertexID start_localid_by_globalid = start_out_edges + size_out_edges;
  VertexID start_localid = start_localid_by_globalid + size_localid_by_globalid;

  globalid_by_index_ = reinterpret_cast<VertexID*>(buf_graph_ + start_globalid);
  in_edges_ = reinterpret_cast<VertexID*>(buf_graph_ + start_in_edges);
  out_edges_ = reinterpret_cast<VertexID*>(buf_graph_ + start_out_edges);
  indegree_ = reinterpret_cast<VertexID*>(buf_graph_ + start_indegree);
  outdegree_ = reinterpret_cast<VertexID*>(buf_graph_ + start_outdegree);
  in_offset_ = reinterpret_cast<VertexID*>(buf_graph_ + start_in_offset);
  out_offset_ = reinterpret_cast<VertexID*>(buf_graph_ + start_out_offset);
}

}  // namespace sics::graph::core::data_structures::graph
