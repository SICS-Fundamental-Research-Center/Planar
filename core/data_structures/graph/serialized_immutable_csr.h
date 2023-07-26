#ifndef GRAPH_SYSTEMS_SERIALIZED_IMMUTABLE_CSR_H
#define GRAPH_SYSTEMS_SERIALIZED_IMMUTABLE_CSR_H

#include <utility>

#include "data_structures/serialized.h"

namespace sics::graph::core::data_structures::graph {

class SerializedImmutableCSR : public Serialized {
 protected:
  // The first item of csr_buffer_: buffer list of meta data;
  // The second item of csr_buffer_: buffer list of subgraph_csr data.
  std::list<std::list<OwnedBuffer>> csr_buffer_;

  // Reader call this function to push buffers into csr_buffer_.
  void ReceiveBuffers(std::list<OwnedBuffer>&& buffers) override {
    csr_buffer_.emplace_back(std::move(buffers));
  };

  // Writer call this function to pop buffers from csr_buffer_ to Disk.
  std::list<OwnedBuffer> PopNextImpl() override {
    // remove the first item of csr_buffer_ and return it
    std::list<OwnedBuffer> buffers = std::move(csr_buffer_.front());
    csr_buffer_.pop_front();
    return buffers;
  }

 public:
  SerializedImmutableCSR() {}
  bool HasNext() const override { return this->csr_buffer_.size() > 0; }

  std::list<std::list<OwnedBuffer>>& get_csr_buffer(){
      return this->csr_buffer_;
  };
};

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_SERIALIZED_IMMUTABLE_CSR_H
