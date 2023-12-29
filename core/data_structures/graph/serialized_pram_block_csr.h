#ifndef GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_SERIALIZED_PRAM_BLOCK_CSR_H_
#define GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_SERIALIZED_PRAM_BLOCK_CSR_H_

#include <list>
#include <utility>

#include "data_structures/serialized.h"

namespace sics::graph::core::data_structures::graph {

class SerializedPramBlockCSRGraph : public Serialized {
 public:
  SerializedPramBlockCSRGraph() = default;
  bool HasNext() const override { return csr_buffer_.size() > 0; }

  void ReceiveBuffers(std::vector<OwnedBuffer>&& buffers) override {
    csr_buffer_.emplace_back(std::move(buffers));
  }

  std::vector<OwnedBuffer>* GetCSRBuffer() { return &csr_buffer_.front(); }

 protected:
  std::list<std::vector<OwnedBuffer>> csr_buffer_;

  std::vector<OwnedBuffer> PopNextImpl() override {
    std::vector<OwnedBuffer> buffers = std::move(csr_buffer_.front());
    csr_buffer_.pop_front();
    return buffers;
  }
};

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_SERIALIZED_PRAM_BLOCK_CSR_H_
