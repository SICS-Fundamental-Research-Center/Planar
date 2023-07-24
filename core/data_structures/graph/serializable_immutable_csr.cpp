#include "serializable_immutable_csr.h"
#include "util/logging.h"

namespace sics::graph::core::data_structures::graph {

std::unique_ptr<Serialized> SerializableImmutableCSR::Serialize(
    common::TaskRunner& runner) {
  // TODO: Implement this.
  // return std::make_unique<SerializedImmutableCSR>();
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
    ParseMetadata(*iter++);
  }
}

void SerializableImmutableCSR::ParseMetadata(
    std::list<OwnedBuffer>& buffer_list) {}

void SerializableImmutableCSR::ParseSubgraphCSR(
    std::list<OwnedBuffer>& buffer_list) {
  OwnedBuffer& buffer = buffer_list.front();
}

}  // namespace sics::graph::core::data_structures::graph