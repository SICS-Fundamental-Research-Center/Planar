#ifndef CORE_DATA_STRUCTURES_GRAPH_SERIALIZED_MUTABLE_CSR_GRAPH_H_
#define CORE_DATA_STRUCTURES_GRAPH_SERIALIZED_MUTABLE_CSR_GRAPH_H_

#include <list>
#include <utility>

#include "data_structures/serialized.h"

namespace sics::graph::core::data_structures::graph {

class SerializedMutableCSRGraph : public Serialized {
 public:
  SerializedMutableCSRGraph() = default;
  bool HasNext() const override { return csr_buffer_.size() > 0; }

  void ReceiveBuffers(std::list<OwnedBuffer>&& buffers) override {
    csr_buffer_.emplace_back(std::move(buffers));
  }

 protected:
  std::list<std::list<OwnedBuffer>> csr_buffer_;

  // TODO: test if unique_ptr is needed
  std::list<OwnedBuffer> PopNextImpl() override {
    std::list<OwnedBuffer> buffers = std::move(csr_buffer_.front());
    csr_buffer_.pop_front();
    return buffers;
  }
};

}  // namespace sics::graph::core::data_structures::graph

#endif  // CORE_DATA_STRUCTURES_GRAPH_SERIALIZED_MUTABLE_CSR_GRAPH_H_