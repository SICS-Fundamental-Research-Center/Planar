#ifndef GRAPH_SYSTEMS_SERIALIZABLE_IMMUTABLE_CSR_H
#define GRAPH_SYSTEMS_SERIALIZABLE_IMMUTABLE_CSR_H

#include "data_structures/csr_graph/serialized_immutable_csr.h"
#include "data_structures/serializable.h"
#include "data_structures/serializable_graph.h"

namespace sics::graph::core::data_structures::csr_graph {

template <typename GID_T, typename VID_T>
class SerializableImmutableCSR : public SerializableGraph<GID_T, VID_T> {
 public:
  SerializableImmutableCSR(const GID_T gid, const VID_T max_vid)
      : SerializableGraph<GID_T, VID_T>(gid), max_vid_(max_vid) {}

  std::unique_ptr<Serialized> Serialize(common::TaskRunner& runner) override {
    // TODO: Implement this.
    return std::make_unique<SerializedImmutableCSR>();
  }

  void Deserialize(common::TaskRunner& runner,
                   Serialized&& serialized) override {
    serialized_immutable_csr_ = std::move(serialized);
    auto iter = serialized_immutable_csr_.csr_buffer_.begin();
    if (iter != serialized_immutable_csr_.csr_buffer_.end()) {
      ParseMetadata(*iter++);
    }
    if (iter != serialized_immutable_csr_.csr_buffer_.end()) {
      ParseSubgraphCSR(*iter++);
    }
  }

  void ParseMetadata(std::list<Buffer>) {
    // Parse metadata from buffer.
  }

  void ParseSubgraphCSR(std::list<Buffer>) {
    // Parse subgraph_csr from buffer.
  }

 protected:
  struct Metadata {
    size_t num_vertice_ = 0;
    size_t num_in_edges_ = 0;
    size_t num_out_edges_ = 0;
  };
  VID_T max_vid_ = 0;
  VID_T aligned_max_vid_ = 0;
  SerializedImmutableCSR serialized_immutable_csr_;

  // serialized data in CSR format.
  VID_T* localid_by_globalid_ = nullptr;
  VID_T* globalid_by_index_ = nullptr;
  VID_T* in_edges_ = nullptr;
  VID_T* out_edges_ = nullptr;
  size_t* indegree_ = nullptr;
  size_t* outdegree_ = nullptr;
  size_t* in_offset_ = nullptr;
  size_t* out_offset_ = nullptr;
};

}  // namespace sics::graph::core::data_structures::csr_graph

#endif  // GRAPH_SYSTEMS_SERIALIZABLE_IMMUTABLE_CSR_H