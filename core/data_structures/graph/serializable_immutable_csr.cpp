#include "serializable_immutable_csr.h"

namespace sics::graph::core::data_structures::graph {

std::unique_ptr<Serialized> SerializableImmutableCSR::Serialize(
    common::TaskRunner& runner) {
  // TODO: Implement this.
  return std::make_unique<SerializedImmutableCSR>();
}

void SerializableImmutableCSR::Deserialize(common::TaskRunner& runner,
                                           Serialized&& serialized) {
  serialized_immutable_csr_ = static_cast<SerializedImmutableCSR&&>(serialized);
  auto csr_buffer = serialized_immutable_csr_.get_csr_buffer();
  auto iter = csr_buffer.begin();
  if (iter != csr_buffer.end()) {
    ParseMetadata(*iter++);
  }
  if (iter != csr_buffer.end()) {
    ParseSubgraphCSR(*iter++);
  }
}

void SerializableImmutableCSR::ParseMetadata(std::list<OwnedBuffer> buffers) {
    
}

void SerializableImmutableCSR::ParseSubgraphCSR(
    std::list<OwnedBuffer> buffers) {}
}  // namespace sics::graph::core::data_structures::graph