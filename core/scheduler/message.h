#ifndef GRAPH_SYSTEMS_MESSAGE_H
#define GRAPH_SYSTEMS_MESSAGE_H

#include <string>

#include "apis/pie.h"
#include "common/blocking_queue.h"
#include "common/types.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"
#include "util/logging.h"

namespace sics::graph::core::scheduler {

struct ReadMessage {
  ReadMessage() = default;
  ReadMessage(const ReadMessage& message) = default;
  // Request fields.
  common::GraphID graph_id = 0;
  common::VertexCount num_vertices = 0;
  int round = 0;

  size_t read_block_size_ = 0;
  size_t num_edge_blocks;
  // Response fields.
  data_structures::Serialized* response_serialized = nullptr;  // initialized in loader

  // Termination flag.
  bool terminated = false;
};

typedef enum {
  kDeserialize = 1,
  kPEval,
  kIncEval,
  kSerialize,
} ExecuteType;

struct ExecuteMessage {
  ExecuteMessage() = default;
  // Request fields.
  common::GraphID graph_id = 0;
  data_structures::Serialized* serialized = nullptr;
  ExecuteType execute_type = kPEval;
  // TODO: add subgraph metadata fields and API program objects.
  data_structures::Serializable* graph = nullptr;
  apis::PIE* app = nullptr;

  // Response fields.
  data_structures::Serializable* response_serializable = nullptr;

  // Termination flag.
  bool terminated = false;
};

struct WriteMessage {
  WriteMessage() = default;
  // Request fields.
  common::GraphID graph_id = 0;
  data_structures::Serializable* serializable = nullptr;
  data_structures::Serialized* serialized = nullptr;
  // TODO: add subgraph metadata fields.

  // Response fields.
  size_t bytes_written = 0;

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
struct fmt::formatter<sics::graph::core::scheduler::Message::Type> {
  typedef sics::graph::core::scheduler::Message::Type MessageType;

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
      default:
        return fmt::format_to(ctx.out(), "UnknownMessageType");
    }
  }
};

#endif  // GRAPH_SYSTEMS_MESSAGE_H
