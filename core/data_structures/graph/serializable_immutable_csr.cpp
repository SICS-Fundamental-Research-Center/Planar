#include "serializable_immutable_csr.h"
#include "common/types.h"
#include "util/logging.h"
#include <cmath>

using VertexID = sics::graph::core::common::VertexID;

#define ALIGNMENT_FACTOR (double)64.0

namespace sics::graph::core::data_structures::graph {

std::unique_ptr<Serialized> SerializableImmutableCSR::Serialize(
    common::TaskRunner& runner) {
  // TODO: Implement this.
  // return std::make_unique<SerializedImmutableCSR>();
}

void SerializableImmutableCSR::Deserialize(common::TaskRunner& runner,
                                           Serialized&& serialized) {
  serialized_immutable_csr_ =
      std::move(static_cast<SerializedImmutableCSR&&>(serialized));
  auto& csr_buffer = serialized_immutable_csr_.get_csr_buffer();
  auto iter = csr_buffer.begin();
  if (iter != csr_buffer.end()) {
    ParseSubgraphCSR(*iter++);
  }
  if (iter != csr_buffer.end()) {
    LOG_ERROR("Only one buffer is expected in the list.");
  }
}

void SerializableImmutableCSR::ParseSubgraphCSR(
    std::list<OwnedBuffer>& buffer_list) {
  // Fetch the OwnedBuffer object.
  uint8_t* buf_graph = buffer_list.front().Get();
  LOG_INFO("[in cpp] loaded size:", buffer_list.front().GetSize());

  VertexID num_vertex = 28;
  size_t sum_in_edges = 61;
  size_t sum_out_edges = 68;

  VertexID max_vertex = 27;
  VertexID aligned_max_vertex =
      std::ceil(max_vertex / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;

  size_t size_localid = sizeof(VertexID) * num_vertex;
  size_t size_globalid = sizeof(VertexID) * num_vertex;
  size_t size_indegree = sizeof(size_t) * num_vertex;
  size_t size_outdegree = sizeof(size_t) * num_vertex;
  size_t size_in_offset = sizeof(size_t) * num_vertex;
  size_t size_out_offset = sizeof(size_t) * num_vertex;
  size_t size_in_edges = sizeof(VertexID) * sum_in_edges;
  size_t size_out_edges = sizeof(VertexID) * sum_out_edges;
  size_t size_localid_by_globalid = sizeof(VertexID) * aligned_max_vertex;

  size_t total_size = size_localid + size_globalid + size_indegree +
                      size_outdegree + size_in_offset + size_out_offset +
                      size_in_edges + size_out_edges + size_localid_by_globalid;

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

  LOG_INFO("[in cpp] total_size:", total_size);

  localid_by_globalid_ = (VertexID*)(buf_graph + start_localid_by_globalid);
  localid_by_index_ = (VertexID*)(buf_graph + start_localid);
  globalid_by_index_ = (VertexID*)(buf_graph + start_globalid);
  in_edges_ = (VertexID*)(buf_graph + start_in_edges);
  out_edges_ = (VertexID*)(buf_graph + start_out_edges);
  indegree_ = (size_t*)(buf_graph + start_indegree);
  outdegree_ = (size_t*)(buf_graph + start_outdegree);
  in_offset_ = (size_t*)(buf_graph + start_in_offset);
  out_offset_ = (size_t*)(buf_graph + start_out_offset);

  for (VertexID i = 0; i < 28; ++i) {
    LOG_INFO("[in cpp] check vertex: ", i);
    LOG_INFO("l_id: ", localid_by_index_[i], "; g_id: ", globalid_by_index_[i],
             "; local_by_global: ", localid_by_globalid_[i],
             "; indegree: ", indegree_[i], "; outdegree: ", outdegree_[i]);
    for (int j = 0; j < indegree_[i]; ++j) {
      LOG_INFO("in_edge: ", in_edges_[in_offset_[i] + j]);
    }
    for (int j = 0; j < outdegree_[i]; ++j) {
      LOG_INFO("out_edge: ", out_edges_[out_offset_[i] + j]);
    }
  }
}

}  // namespace sics::graph::core::data_structures::graph