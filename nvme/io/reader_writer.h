#ifndef GRAPH_SYSTEMS_NVME_IO_READER_WRITER_H_
#define GRAPH_SYSTEMS_NVME_IO_READER_WRITER_H_

#include "core/common/multithreading/task_runner.h"
#include "nvme/scheduler/message.h"

namespace sics::graph::nvme::io {

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
                    core::common::TaskRunner* runner = nullptr) = 0;

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
                     core::common::TaskRunner* runner = nullptr) = 0;
};

}  // namespace sics::graph::nvme::io

#endif  // GRAPH_SYSTEMS_NVME_IO_READER_WRITER_H_
