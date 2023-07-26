#ifndef CORE_DATA_STRUCTURES_SERIALIZABLE_H_
#define CORE_DATA_STRUCTURES_SERIALIZABLE_H_

#include <memory>

#include "common/multithreading/task_runner.h"
#include "data_structures/serialized.h"

namespace sics::graph::core::data_structures {

class Serializable {
 public:
  virtual std::unique_ptr<Serialized> Serialize(common::TaskRunner& runner) = 0;
  virtual void Deserialize(common::TaskRunner& runner,
                           Serialized&& serialized) = 0;
};

}  // namespace sics::graph::core::data_structures

#endif  // CORE_DATA_STRUCTURES_SERIALIZABLE_H_
