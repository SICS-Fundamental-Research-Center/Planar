#ifndef GRAPH_SYSTEMS_SERIALIZABLE_H
#define GRAPH_SYSTEMS_SERIALIZABLE_H

#include <list>

#include "data_structures/buffer.h"
#include "common/multithreading/task_runner.h"


namespace sics::graph::core::data_structures {

class Serialized {
 public:
  virtual bool HasNext() const = 0;

  bool IsComplete() const;

  void ReceiveBuffers(std::list<Buffer>&& buffers) {
    ReceiveBuffersImpl(std::move(buffers));
    is_complete_ = true;
  }

  std::list<Buffer> PopNext() {
    is_complete_ = false;
    return PopNextImpl();
  }

 protected:
  virtual void ReceiveBuffersImpl(std::list<Buffer>&& buffers) = 0;
  virtual std::list<Buffer> PopNextImpl() = 0;

 protected:
  bool is_complete_;
};


template <typename SType>
class Serializable {
 public:
  struct Metadata {};

 public:
  // Static assert here.

 public:
  virtual SType Serialize(common::TaskRunner& runner) = 0;
  virtual void Deserialize(common::TaskRunner& runner,
                           const Metadata& metadata,
                           SType&& serialized) = 0;
};


} // namespace sics::graph::core::data_structures

#endif  // GRAPH_SYSTEMS_SERIALIZABLE_H
