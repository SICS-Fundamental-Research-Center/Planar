#ifndef GRAPH_SYSTEMS_MESSAGE_H
#define GRAPH_SYSTEMS_MESSAGE_H

namespace sics::graph::core::scheduler {

enum MessageType {
  kReadRequest = 1,
  kExecuteRequest,
  kWriteRequest,

  kReadResponse = 11,
  kExecuteResponse,
  kWriteResponse,
};

class Message {
 public:
  explicit Message(MessageType type) : type_(type) {}

  virtual ~Message() = default;

  [[nodiscard]]
  MessageType get_type() const {
    return type_;
  }

 protected:
  const MessageType type_;
};

}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_MESSAGE_H
