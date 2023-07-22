#ifndef GRAPH_SYSTEMS_SERIALIZABLE_H
#define GRAPH_SYSTEMS_SERIALIZABLE_H

#include "common/multithreading/task_runner.h"
#include "data_structures/buffer.h"
#include <list>
#include <memory>

namespace sics::graph::core::data_structures {

class Serialized {
 public:
  virtual bool HasNext() const = 0;

  virtual void ReceiveBuffers(std::list<Buffer>&& buffers) = 0;

  std::list<Buffer> PopNext() {
    is_complete_ = false;
    return PopNextImpl();
  }

  bool IsComplete() const { return is_complete_; }

  void SetComplete() { is_complete_ = true; }

 protected:
  virtual std::list<Buffer> PopNextImpl() = 0;

 protected:
  bool is_complete_;
};

class Serializable {
 public:
  struct Metadata {};

 public:
  // Static assert here.

 public:
  virtual std::unique_ptr<Serialized> Serialize(common::TaskRunner& runner) = 0;
  virtual void Deserialize(common::TaskRunner& runner, const Metadata& metadata,
                           Serialized&& serialized) = 0;
};

}  // namespace sics::graph::core::data_structures

#endif  // GRAPH_SYSTEMS_SERIALIZABLE_H
