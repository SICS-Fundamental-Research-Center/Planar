#ifndef CORE_IO_RW_INTERFACE_H_
#define CORE_IO_RW INTERFACE_H_

#include "scheduler/message.h"

namespace sics::graph::core::io {

class Reader {
 protected:
  using ReadMessage = scheduler::ReadMessage;

 public:
  virtual ~Reader() = default;
  virtual void Read(ReadMessage* message) = 0;
};

class Writer {
 protected:
  using WriteMessage = scheduler::WriteMessage;

 public:
  virtual ~Writer() = default;
  virtual void Write(WriteMessage* message) = 0;
};

}  // namespace sics::graph::core::io

#endif  // CORE_IO_RW_INTERFACE_H_
