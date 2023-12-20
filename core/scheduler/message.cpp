#include "message.h"

namespace sics::graph::core::scheduler {

Message::Message(const ReadMessage& message)
    : type_(kRead), message_(message) {}

Message::Message(const ExecuteMessage& message)
    : type_(kExecute), message_(message) {}

Message::Message(const WriteMessage& message)
    : type_(kWrite), message_(message) {}

void Message::Set(const ReadMessage& message) {
  type_ = kRead;
  message_.read_message = message;
}

void Message::Set(const ExecuteMessage& message) {
  type_ = kExecute;
  message_.execute_message = message;
}

void Message::Set(const WriteMessage& message) {
  type_ = kWrite;
  message_.write_message = message;
}

void Message::Get(ReadMessage* message) const {
  if (get_type() != kRead) {
    LOGF_WARN("Message type mismatch: expected {}, got {}", kRead, get_type());
  }
  *message = message_.read_message;
}

void Message::Get(ExecuteMessage* message) const {
  if (get_type() != kExecute) {
    LOGF_WARN("Message type mismatch: expected {}, got {}", kExecute,
              get_type());
  }
  *message = message_.execute_message;
}

void Message::Get(WriteMessage* message) const {
  if (get_type() != kWrite) {
    LOGF_WARN("Message type mismatch: expected {}, got {}", kWrite, get_type());
  }
  *message = message_.write_message;
}

}  // namespace sics::graph::core::scheduler