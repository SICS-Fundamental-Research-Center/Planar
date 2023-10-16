#ifndef CORE_IO_RW_INTERFACE_H_
#define CORE_IO_RW_INTERFACE_H_

#include "common/multithreading/task_runner.h"
#include "scheduler/message.h"

namespace xyz::graph::core::io {

class Reader {
 protected:
  using ReadMessage = scheduler::ReadMessage;

 public:
  virtual ~Reader() = default;

  // Read a subgraph, as specified by the message, from the storage.
  //
  // If the runner is not null, the read operation will be executed in the
  // caller thread; otherwise, it will be executed by the provided task runner.
  virtual void Read(ReadMessage* message,
                    common::TaskRunner* runner = nullptr) = 0;

  virtual size_t SizeOfReadNow() { return 0; }
};

class Writer {
 protected:
  using WriteMessage = scheduler::WriteMessage;

 public:
  virtual ~Writer() = default;

  // Write a subgraph, as specified by the message, to the storage.
  //
  // If the runner is not null, the write operation will be executed in the
  // caller thread; otherwise, it will be executed by the provided task runner.
  virtual void Write(WriteMessage* message,
                     common::TaskRunner* runner = nullptr) = 0;
};

}  // namespace xyz::graph::core::io

#endif  // CORE_IO_RW_INTERFACE_H_
