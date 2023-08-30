#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::data_structures::graphs {
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
    // Parse out edge label
    ParseOutedgeLabel(*iter++);
  }
  if (iter != csr_buffer.end()) {
    // Parse vertex label
    ParseVertexLabel(*iter++);
  }
  if (iter != csr_buffer.end()) {
    // Parse vertex attribute
    ParseVertexAttribute(*iter++);
  }
}

void MiniCleanCSRGraph::ParseSubgraphCSR(
    const std::vector<OwnedBuffer>& buffer_list) {
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

void MiniCleanCSRGraph::ParseOutedgeLabel(
    const std::vector<OwnedBuffer>& buffer_list) {
  out_edge_label_base_pointer_ =
      reinterpret_cast<EdgeLabel*>(buffer_list.front().Get());
}
void MiniCleanCSRGraph::ParseVertexLabel(
    const std::vector<OwnedBuffer>& buffer_list) {
  vertex_label_base_pointer_ =
      reinterpret_cast<VertexLabel*>(buffer_list.front().Get());
}
void MiniCleanCSRGraph::ParseVertexAttribute(
    const std::vector<OwnedBuffer>& buffer_list) {
  vertex_attribute_base_pointer_ =
      reinterpret_cast<VertexID*>(buffer_list.front().Get());

  auto ptr = vertex_attribute_base_pointer_;

  std::vector<VertexID> vertex_attr_offset(num_vertices_);
  std::vector<VertexAttributeValue> vertex_attr_value;
  
  VertexID crt_offset = 0;

  for (size_t i = 0; i < num_vertices_; i++) {
    vertex_attr_offset[i] = crt_offset;
    // Check vertex id.
    if (i != *ptr) {
      LOG_ERROR("Vertex id does not match.");
    }

    // Retrieve vertex label
    VertexLabel label = GetVertexLabelByLocalID(i);

    // Retrieve attr count.
    ptr++;
    auto attr_count = *ptr;

    // Check attr count.
    if (attr_count > 0 && label != 1) {
      LOG_WARN("Attr count: ", attr_count, ", label: ", label);
    }
    if (attr_count > 2 && label == 1) {
      LOG_WARN("Attr count: ", attr_count, ", label: ", label);
    }

    // Initialize vertex attr value.
    if (label == 1) {
      vertex_attr_value.push_back(2);
      vertex_attr_value.push_back(MAX_VERTEX_ATTRIBUTE_VALUE);
      vertex_attr_value.push_back(MAX_VERTEX_ATTRIBUTE_VALUE);
      crt_offset += 3;
    } else {
      vertex_attr_value.push_back(0);
      crt_offset += 1;
    }

    // Check vertex attr and value.
    for (size_t j = 0; j < attr_count; j++) {
      ptr++;
      auto attr_id = *ptr;
      ptr++;
      auto attr_val = *ptr;
      vertex_attr_value[crt_offset - 3 + attr_id] = attr_val;
      if (attr_id > 2) {
        LOG_WARN("Vertex attr id is larger than 2.");
      }
      if (attr_val == MAX_VERTEX_ATTRIBUTE_VALUE) {
        LOG_WARN("Vertex attr value is MAX_VERTEX_ATTRIBUTE_VALUE.");
      }
    }
    // Move to next vertex.
    ptr++;
  }

  int a = 0;
}

}  // namespace sics::graph::miniclean::data_structures::graphs