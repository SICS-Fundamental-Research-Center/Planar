#include "immutable_csr_graph.h"

namespace sics::graph::core::data_structures::graph {

std::unique_ptr<Serialized> ImmutableCSRGraph::Serialize(
    const common::TaskRunner& runner) {
  // TODO: Implement this.
  return nullptr;
}

void ImmutableCSRGraph::Deserialize(const common::TaskRunner& runner,
                                           Serialized&& serialized) {
  // TODO: Submit to the task runner.
  SerializedImmutableCSRGraph serialized_immutable_csr_ =
      std::move(static_cast<SerializedImmutableCSRGraph&&>(serialized));
  auto& csr_buffer = serialized_immutable_csr_.get_csr_buffer();
  ParseSubgraphCSR(csr_buffer.front());
  int a = 0;
  // auto iter = csr_buffer.begin();
  // ParseSubgraphCSR(*iter);
  // if (iter != csr_buffer.end()) {
    
  //   // if (iter++ != csr_buffer.end()) {
  //   //   // LOG_ERROR("Only one buffer is expected in the list.");
  //   // }
  // }
}

void ImmutableCSRGraph::ParseSubgraphCSR(
    const std::list<OwnedBuffer>& buffer_list) {
  // Fetch the OwnedBuffer object.
  buf_graph_ = buffer_list.front().Get();
  // LOG_INFO("[in cpp] loaded size:", buffer_list.front().GetSize());

  VertexID aligned_max_vertex =
      std::ceil(csr_config_.max_vertex / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;

  size_t size_localid = sizeof(VertexID) * csr_config_.num_vertex;
  size_t size_globalid = sizeof(VertexID) * csr_config_.num_vertex;
  size_t size_indegree = sizeof(size_t) * csr_config_.num_vertex;
  size_t size_outdegree = sizeof(size_t) * csr_config_.num_vertex;
  size_t size_in_offset = sizeof(size_t) * csr_config_.num_vertex;
  size_t size_out_offset = sizeof(size_t) * csr_config_.num_vertex;
  size_t size_in_edges = sizeof(VertexID) * csr_config_.sum_in_edges;
  size_t size_out_edges = sizeof(VertexID) * csr_config_.sum_out_edges;
  size_t size_localid_by_globalid = sizeof(VertexID) * aligned_max_vertex;

  // size_t total_size = size_localid + size_globalid + size_indegree +
  //                     size_outdegree + size_in_offset + size_out_offset +
  //                     size_in_edges + size_out_edges + size_localid_by_globalid;

  size_t start_globalid = 0;
  // size_t start_globalid = start_localid + size_localid;
  size_t start_indegree = start_globalid + size_globalid;
  size_t start_outdegree = start_indegree + size_indegree;
  size_t start_in_offset = start_outdegree + size_outdegree;
  size_t start_out_offset = start_in_offset + size_in_offset;
  size_t start_in_edges = start_out_offset + size_out_offset;
  size_t start_out_edges = start_in_edges + size_in_edges;
  size_t start_localid_by_globalid = start_out_edges + size_out_edges;
  size_t start_localid = start_localid_by_globalid + size_localid_by_globalid;

  // LOG_INFO("[in cpp] total_size:", total_size);

  localid_by_globalid_ = reinterpret_cast<VertexID*> (buf_graph_ + start_localid_by_globalid);
  localid_by_index_ = reinterpret_cast<VertexID*>(buf_graph_ + start_localid);
  globalid_by_index_ = reinterpret_cast<VertexID*>(buf_graph_ + start_globalid);
  in_edges_ = reinterpret_cast<VertexID*>(buf_graph_ + start_in_edges);
  out_edges_ = reinterpret_cast<VertexID*>(buf_graph_ + start_out_edges);
  indegree_ = reinterpret_cast<size_t*>(buf_graph_ + start_indegree);
  outdegree_ = reinterpret_cast<size_t*>(buf_graph_ + start_outdegree);
  in_offset_ = reinterpret_cast<size_t*>(buf_graph_ + start_in_offset);
  out_offset_ = reinterpret_cast<size_t*>(buf_graph_ + start_out_offset);

  // LOG_INFO("[in cpp] globalid_by_index:", globalid_by_index_);

  // for (VertexID i = 0; i < 28; ++i) {
  //   LOG_INFO("[in cpp] check vertex: ", i);
  //   LOG_INFO("l_id: ", localid_by_index_[i], "; g_id: ", globalid_by_index_[i],
  //            "; local_by_global: ", localid_by_globalid_[globalid_by_index_[i]],
  //            "; outdegree: ", outdegree_[i], "; indegree: ", indegree_[i]);
  //   for (int j = 0; j < indegree_[i]; ++j) {
  //     LOG_INFO("in_edge: ", in_edges_[in_offset_[i] + j]);
  //   }
  //   for (int j = 0; j < outdegree_[i]; ++j) {
  //     LOG_INFO("out_edge: ", out_edges_[out_offset_[i] + j]);
  //   }
  // }
}

}  // namespace sics::graph::core::data_structures::graph
