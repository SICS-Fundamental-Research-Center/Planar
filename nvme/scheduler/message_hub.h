#ifndef GRAPH_SYSTEMS_NVME_SCHEDULER_MESSAGE_HUB_H_
#define GRAPH_SYSTEMS_NVME_SCHEDULER_MESSAGE_HUB_H_

#include "core/common/blocking_queue.h"
#include "nvme/scheduler/message.h"

namespace sics::graph::nvme::scheduler {

typedef core::common::BlockingQueue<ReadMessage> ReaderQueue;
typedef core::common::BlockingQueue<ExecuteMessage> ExecutorQueue;
typedef core::common::BlockingQueue<WriteMessage> WriterQueue;
typedef core::common::BlockingQueue<Message> ResponseQueue;

class MessageHub {
 public:
  MessageHub() = default;
  ~MessageHub() = default;

  void SendRequest(const ReadMessage& message) { reader_q_.Push(message); }

  void SendRequest(const ExecuteMessage& message) { executor_q_.Push(message); }

  void SendRequest(const WriteMessage& message) { writer_q_.Push(message); }

  void SendResponse(const Message& message) { response_q_.Push(message); }

  Message GetResponse() { return response_q_.PopOrWait(); }

  ReaderQueue* get_reader_queue() { return &reader_q_; }
  ExecutorQueue* get_executor_queue() { return &executor_q_; }
  WriterQueue* get_writer_queue() { return &writer_q_; }
  ResponseQueue* get_response_queue() { return &response_q_; }

 private:
  ReaderQueue reader_q_;
  ExecutorQueue executor_q_;
  WriterQueue writer_q_;
  ResponseQueue response_q_;
};

}  // namespace sics::graph::nvme::scheduler

#endif  // GRAPH_SYSTEMS_NVME_SCHEDULER_MESSAGE_HUB_H_
