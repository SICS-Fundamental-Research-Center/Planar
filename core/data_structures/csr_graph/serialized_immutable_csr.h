#ifndef GRAPH_SYSTEMS_SERIALIZED_IMMUTABLE_CSR_H
#define GRAPH_SYSTEMS_SERIALIZED_IMMUTABLE_CSR_H

#include "data_structures/serializable.h"

namespace sics::graph::core::data_structures::csr_graph {

class SerializedImmutableCSR : public Serialized {
 protected:
  // The first item of csr_buffer_: buffer list of meta data;
  // The second item of csr_buffer_: buffer list of subgraph_csr data.
  std::list<std::list<Buffer>> csr_buffer_;

  // Reader call this function to push buffers into csr_buffer_.
  void ReceiveBuffers(std::list<Buffer>&& buffers) override {
    this->csr_buffer_.push_back(std::move(buffers));
  };

  // Writer call this function to pop buffers from csr_buffer_ to Disk.
  std::list<Buffer> PopNextImpl() override {
    std::list<Buffer> result = std::move(this->csr_buffer_.front());
    this->csr_buffer_.pop_front();
    return result;
  };

 public:
  SerializedImmutableCSR(){};
  bool HasNext() const override { return this->csr_buffer_.size() > 0; };
};

}  // namespace sics::graph::core::data_structures::csr_graph

#endif  // GRAPH_SYSTEMS_SERIALIZED_IMMUTABLE_CSR_H
