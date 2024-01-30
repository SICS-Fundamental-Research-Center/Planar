#include "nvme/scheduler/message.h"

namespace sics::graph::nvme::scheduler {

Message::Message(const ReadMessage& message)
    : type_(kRead), read_message(message) {}

Message::Message(const ExecuteMessage& message)
    : type_(kExecute), execute_message(message) {}

Message::Message(const WriteMessage& message)
    : type_(kWrite), write_message(message) {}

// Message::~Message() {
//   switch (type_) {
//     case kRead:
//     case kWrite:
//       break;
//     case kExecute:
//       this->execute_message.~ExecuteMessage();
//       break;
//     default:
//       break;
//   }
// }

// Message::Message(const Message& message) {
//   type_ = message.type_;
//   switch (type_) {
//     case kRead:
//       this->read_message = message.read_message;
//       break;
//     case kExecute:
//       this->execute_message = message.execute_message;
//       break;
//     case kWrite:
//       this->write_message = message.write_message;
//       break;
//     default:
//       break;
//   }
// }

void Message::Set(const ReadMessage& message) {
  type_ = kRead;
  this->read_message = message;
}

void Message::Set(const ExecuteMessage& message) {
  type_ = kExecute;
  this->execute_message = message;
}

void Message::Set(const WriteMessage& message) {
  type_ = kWrite;
  this->write_message = message;
}

void Message::Get(ReadMessage* message) const {
  if (get_type() != kRead) {
    LOGF_WARN("Message type mismatch: expected {}, got {}", kRead, get_type());
  }
  *message = this->read_message;
}

void Message::Get(ExecuteMessage* message) const {
  if (get_type() != kExecute) {
    LOGF_WARN("Message type mismatch: expected {}, got {}", kExecute,
              get_type());
  }
  *message = this->execute_message;
}

void Message::Get(WriteMessage* message) const {
  if (get_type() != kWrite) {
    LOGF_WARN("Message type mismatch: expected {}, got {}", kWrite, get_type());
  }
  *message = this->write_message;
}

}  // namespace sics::graph::nvme::scheduler