#include "data_structures/graph/immutable_csr_graph.h"

namespace sics::graph::core::data_structures::graph {

std::unique_ptr<Serialized> ImmutableCSRGraph::Serialize(
    const common::TaskRunner& runner) {
  return std::unique_ptr<Serialized>(static_cast<Serialized*>(
      serialized_.release()));
}

void ImmutableCSRGraph::Deserialize(const common::TaskRunner& runner,
                                    std::unique_ptr<Serialized>&& serialized) {
  // TODO(bwc): Submit to the task runner.
  auto new_serialized = std::unique_ptr<SerializedImmutableCSRGraph>
      (static_cast<SerializedImmutableCSRGraph*>(serialized.release()));
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

  VertexID aligned_max_vertex = (csr_config_.max_vertex + 63) / 64 * 64;

  size_t size_globalid = sizeof(VertexID) * csr_config_.num_vertex;
  size_t size_indegree = sizeof(size_t) * csr_config_.num_vertex;
  size_t size_outdegree = sizeof(size_t) * csr_config_.num_vertex;
  size_t size_in_offset = sizeof(size_t) * csr_config_.num_vertex;
  size_t size_out_offset = sizeof(size_t) * csr_config_.num_vertex;
  size_t size_in_edges = sizeof(VertexID) * csr_config_.sum_in_edges;
  size_t size_out_edges = sizeof(VertexID) * csr_config_.sum_out_edges;
  size_t size_localid_by_globalid = sizeof(VertexID) * aligned_max_vertex;

  size_t start_globalid = 0;
  size_t start_indegree = start_globalid + size_globalid;
  size_t start_outdegree = start_indegree + size_indegree;
  size_t start_in_offset = start_outdegree + size_outdegree;
  size_t start_out_offset = start_in_offset + size_in_offset;
  size_t start_in_edges = start_out_offset + size_out_offset;
  size_t start_out_edges = start_in_edges + size_in_edges;
  size_t start_localid_by_globalid = start_out_edges + size_out_edges;
  size_t start_localid = start_localid_by_globalid + size_localid_by_globalid;

  localid_by_globalid_ = reinterpret_cast<VertexID*>(buf_graph_ + start_localid_by_globalid);
  localid_by_index_ = reinterpret_cast<VertexID*>(buf_graph_ + start_localid);
  globalid_by_index_ = reinterpret_cast<VertexID*>(buf_graph_ + start_globalid);
  in_edges_ = reinterpret_cast<VertexID*>(buf_graph_ + start_in_edges);
  out_edges_ = reinterpret_cast<VertexID*>(buf_graph_ + start_out_edges);
  indegree_ = reinterpret_cast<size_t*>(buf_graph_ + start_indegree);
  outdegree_ = reinterpret_cast<size_t*>(buf_graph_ + start_outdegree);
  in_offset_ = reinterpret_cast<size_t*>(buf_graph_ + start_in_offset);
  out_offset_ = reinterpret_cast<size_t*>(buf_graph_ + start_out_offset);
}
}  // namespace sics::graph::core::data_structures::graph
