#ifndef GRAPH_SYSTEMS_MESSAGE_HUB_H
#define GRAPH_SYSTEMS_MESSAGE_HUB_H

#include "common/blocking_queue.h"
#include "scheduler/message.h"

namespace sics::graph::core::scheduler {

typedef common::BlockingQueue<ReadMessage> ReaderQueue;
typedef common::BlockingQueue<ExecuteMessage> ExecutorQueue;
typedef common::BlockingQueue<WriteMessage> WriterQueue;
typedef common::BlockingQueue<Message> ResponseQueue;

class MessageHub {
 public:
  MessageHub() = default;
  ~MessageHub() = default;

  void SendRequest(const ReadMessage& message) {
    reader_q_.Push(message);
  }

  void SendRequest(const ExecuteMessage& message) {
    executor_q_.Push(message);
  }

  void SendRequest(const WriteMessage& message) {
    writer_q_.Push(message);
  }

  Message GetResponse() {
    return response_q_.PopOrWait();
  }

  ReaderQueue* get_reader_queue() { return &reader_q_; }
  ExecutorQueue* get_executor_queue() { return &executor_q_; }
  WriterQueue* get_writer_queue() { return &writer_q_; }

 private:
  ReaderQueue reader_q_;
  ExecutorQueue executor_q_;
  WriterQueue writer_q_;
  ResponseQueue response_q_;
};

}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_MESSAGE_HUB_H
