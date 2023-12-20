#ifndef CORE_DATA_STRUCTURES_SERIALIZABLE_H_
#define CORE_DATA_STRUCTURES_SERIALIZABLE_H_

#include <memory>

#include "common/multithreading/task_runner.h"
#include "common/types.h"
#include "data_structures/serialized.h"

namespace sics::graph::core::data_structures {

// This class defines the Serializable interface, which is used to serialize and
// deserialize objects. The Serializable interface provides methods for
// serializing an object to a Serialized object and deserializing an object from
// a Serialized object.
class Serializable {
 public:
  virtual ~Serializable() = default;

  // Serialize the `Serializable` object to a `Serialized` object.
  // This function will submit the serialization task to the given TaskRunner
  virtual std::unique_ptr<Serialized> Serialize(
      const common::TaskRunner& runner) = 0;

  // Deserialize the `Serializable` object from a `Serialized` object.
  // This function will submit the deserialization task to the given TaskRunner
  virtual void Deserialize(const common::TaskRunner& runner,
                           std::unique_ptr<Serialized>&& serialized) = 0;

  virtual common::GraphID GetGraphID() const { return INVALID_GRAPH_ID; };
};

}  // namespace sics::graph::core::data_structures

#endif  // CORE_DATA_STRUCTURES_SERIALIZABLE_H_
