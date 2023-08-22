#ifndef CORE_DATA_STRUCTURES_GRAPH_SERIALIZED_IMMUTABLE_CSR_GRAPH_H_
#define CORE_DATA_STRUCTURES_GRAPH_SERIALIZED_IMMUTABLE_CSR_GRAPH_H_

#include <iostream>
#include <list>
#include <utility>

#include "data_structures/serialized.h"

namespace sics::graph::core::data_structures::graph {

class SerializedImmutableCSRGraph : public Serialized {
 protected:
  // TODO(bwc): Should we adopt list of list?
  // `csr_buffer` stores the serialized data in CSR format. Each `csr_buffer` corresponds to a subgraph.
  // Each item of `csr_buffer` is a list of buffers that correspond to a subgraph file.
  std::list<std::list<OwnedBuffer>> csr_buffer_;

  // Writer call this function to pop buffers from csr_buffer_ to Disk.
  std::list<OwnedBuffer> PopNextImpl() override {
    // move the first buffer to the return list
    std::list<OwnedBuffer> buffers = std::move(csr_buffer_.front());
    csr_buffer_.pop_front();
    return buffers;
  }

 public:
  SerializedImmutableCSRGraph() {}
  bool HasNext() const override { return this->csr_buffer_.size() > 0; }

  // Reader call this function to push buffers into csr_buffer_.
  void ReceiveBuffers(std::list<OwnedBuffer>&& buffers) override {
    csr_buffer_.emplace_back(std::move(buffers));
  };

  std::list<std::list<OwnedBuffer>>& GetCSRBuffer() {
    return this->csr_buffer_;
  }
};

}  // namespace sics::graph::core::data_structures::graph

#endif  // CORE_DATA_STRUCTURES_GRAPH_SERIALIZED_IMMUTABLE_CSR_GRAPH_H_
