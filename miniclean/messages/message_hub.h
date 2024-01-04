#ifndef MINICLEAN_MESSAGES_MESSAGE_HUB_H_
#define MINICLEAN_MESSAGES_MESSAGE_HUB_H_

#include "core/common/blocking_queue.h"
#include "miniclean/messages/message.h"

namespace sics::graph::miniclean::messages {

typedef sics::graph::core::common::BlockingQueue<ReadMessage> ReaderQueue;
typedef sics::graph::core::common::BlockingQueue<ExecuteMessage> ExecutorQueue;
typedef sics::graph::core::common::BlockingQueue<WriteMessage> WriterQueue;
typedef sics::graph::core::common::BlockingQueue<Message> ResponseQueue;

class MessageHub {
 public:
  MessageHub() = default;
  ~MessageHub() = default;

  void SendRequest(const ReadMessage& message) { reader_q_.Push(message); }

  void SendRequest(const ExecuteMessage& message) { executor_q_.Push(message); }

  void SendRequest(const WriteMessage& message) { writer_q_.Push(message); }

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

}  // namespace sics::graph::miniclean::messages

#endif  // MINICLEAN_MESSAGES_MESSAGE_HUB_H_
