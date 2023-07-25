#ifndef GRAPH_SYSTEMS_SERIALIZED_IMMUTABLE_CSR_H
#define GRAPH_SYSTEMS_SERIALIZED_IMMUTABLE_CSR_H

#include "common/types.h"
#include "data_structures/serialized.h"
#include "util/logging.h"
#include <iostream>

#define ALIGNMENT_FACTOR (double)64.0

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
    // TODO: implement this function.
    // return std::list<OwnedBuffer>();
  }

 public:
  SerializedImmutableCSR() {}
  bool HasNext() const override { return this->csr_buffer_.size() > 0; }

  std::list<std::list<OwnedBuffer>>& get_csr_buffer() {
    return this->csr_buffer_;
  };
};

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_SERIALIZED_IMMUTABLE_CSR_H
