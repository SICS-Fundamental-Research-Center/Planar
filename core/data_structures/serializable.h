#ifndef GRAPH_SYSTEMS_SERIALIZABLE_H
#define GRAPH_SYSTEMS_SERIALIZABLE_H

#include "common/multithreading/task_runner.h"
#include "data_structures/serialized.h"
#include <memory>

namespace sics::graph::core::data_structures {

class Serializable {
 public:
  struct Metadata {};

 public:
  // Static assert here.

 public:
  virtual std::unique_ptr<Serialized> Serialize(common::TaskRunner& runner) = 0;
  virtual void Deserialize(common::TaskRunner& runner,
                           Serialized&& serialized) = 0;
};

}  // namespace sics::graph::core::data_structures

#endif  // GRAPH_SYSTEMS_SERIALIZABLE_H
