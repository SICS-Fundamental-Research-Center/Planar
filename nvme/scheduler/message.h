#ifndef GRAPH_SYSTEMS_NVME_SCHEDULER_MESSAGE_H_
#define GRAPH_SYSTEMS_NVME_SCHEDULER_MESSAGE_H_

#include <string>

#include "apis/pie.h"
#include "core/common/blocking_queue.h"
#include "core/common/types.h"
#include "core/data_structures/serializable.h"
#include "core/data_structures/serialized.h"
#include "core/util/logging.h"

namespace sics::graph::nvme::scheduler {

struct ReadMessage {
  ReadMessage() = default;
  ReadMessage(const ReadMessage& message) = default;
  // Request fields.
  core::common::GraphID graph_id;
  core::common::VertexCount num_vertices;
  int round;

  // Response fields.
  core::data_structures::Serialized* response_serialized;  // initialized in loader

  // Termination flag.
  bool terminated = false;
};

typedef enum {
  kDeserialize = 1,
  kCompute,
  kSerialize,
} ExecuteType;

struct ExecuteMessage {
  ExecuteMessage() = default;
  // Request fields.
  core::common::GraphID graph_id;
  core::data_structures::Serialized* serialized;
  ExecuteType execute_type = kCompute;
  // TODO: add subgraph metadata fields and API program objects.
  core::data_structures::Serializable* graph;
  core::apis::PIE* app;

  // Response fields.
  core::data_structures::Serializable* response_serializable;

  // Termination flag.
  bool terminated = false;
};

struct WriteMessage {
  WriteMessage() = default;
  // Request fields.
  core::common::GraphID graph_id;
  core::data_structures::Serializable* serializable;
  core::data_structures::Serialized* serialized;
  // TODO: add subgraph metadata fields.

  // Response fields.
  size_t bytes_written;

  // Termination flag.
  bool terminated = false;
};

// Basically a union of all type of messages.
class Message {
 public:
  typedef enum {
    kRead = 1,
    kExecute,
    kWrite,
  } Type;

 public:
  explicit Message(const ReadMessage& message);
  explicit Message(const ExecuteMessage& message);
  explicit Message(const WriteMessage& message);
  ~Message() = default;

  void Set(const ReadMessage& message);
  void Set(const ExecuteMessage& message);
  void Set(const WriteMessage& message);
  void Get(ReadMessage* message) const;
  void Get(ExecuteMessage* message) const;
  void Get(WriteMessage* message) const;

  [[nodiscard]] Type get_type() const { return type_; }

  [[nodiscard]] bool is_terminated() const {
    switch (type_) {
      case kRead:
        return message_.read_message.terminated;
      case kExecute:
        return message_.execute_message.terminated;
      case kWrite:
        return message_.write_message.terminated;
      default:
        return false;
    }
  }

 private:
  Type type_;

  union Messages {
    ReadMessage read_message;
    ExecuteMessage execute_message;
    WriteMessage write_message;
    Messages(const ReadMessage& message) { read_message = message; };
    Messages(const ExecuteMessage& message) { execute_message = message; };
    Messages(const WriteMessage& message) { write_message = message; };
  } message_;
};

}  // namespace sics::graph::core::scheduler

// The following snippet helps the logger to format the MessageType enum.
template <>
struct fmt::formatter<sics::graph::nvme::scheduler::Message::Type> {
  typedef sics::graph::nvme::scheduler::Message::Type MessageType;

  constexpr auto parse(::fmt::format_parse_context& ctx)
      -> ::fmt::format_parse_context::iterator {
    return ctx.begin();
  }

  auto format(const MessageType& type, ::fmt::format_context& ctx) const
      -> fmt::format_context::iterator {
    switch (type) {
      case MessageType::kRead:
        return fmt::format_to(ctx.out(), "ReadMessage");
      case MessageType::kExecute:
        return fmt::format_to(ctx.out(), "ExecuteMessage");
      case MessageType::kWrite:
        return fmt::format_to(ctx.out(), "WriteMessage");
    }
  }
};

#endif  // GRAPH_SYSTEMS_NVME_SCHEDULER_MESSAGE_H_
