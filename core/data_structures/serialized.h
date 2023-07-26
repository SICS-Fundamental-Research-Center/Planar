#ifndef CORE_DATA_STRUCTURES_SERIALIZED_H_
#define CORE_DATA_STRUCTURES_SERIALIZED_H_

#include <list>

#include "data_structures/buffer.h"

namespace sics::graph::core::data_structures {

class Serialized {
 public:
  virtual bool HasNext() const = 0;

  virtual void ReceiveBuffers(std::list<OwnedBuffer>&& buffers) = 0;

  std::list<OwnedBuffer> PopNext() {
    is_complete_ = false;
    return PopNextImpl();
  }

  bool IsComplete() const { return is_complete_; }

  void SetComplete() { is_complete_ = true; }

 protected:
  virtual std::list<OwnedBuffer> PopNextImpl() = 0;

 protected:
  bool is_complete_ = false;
};

}  // namespace sics::graph::core::data_structures

#endif  // CORE_DATA_STRUCTURES_SERIALIZED_H_