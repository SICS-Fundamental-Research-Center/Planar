#ifndef GRAPH_SYSTEMS_MESSAGE_H
#define GRAPH_SYSTEMS_MESSAGE_H

#include <fmt/core.h>

#include "common/blocking_queue.h"
#include "common/types.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"
#include "util/logging.h"


namespace sics::graph::core::scheduler {

struct ReadMessage {
  // Request fields.
  common::GraphID graph_id;
  // TODO: add subgraph metadata fields.

  // Response fields.
  data_structures::Serialized* response_serialized;
};

struct ExecuteMessage {
  // Request fields.
  common::GraphID graph_id;
  data_structures::Serialized* serialized;
  // TODO: add subgraph metadata fields and API program objects.

  // Response fields.
  data_structures::Serializable* response_serializable;
};

struct WriteMessage {
  // Request fields.
  common::GraphID graph_id;
  data_structures::Serializable* serializable;
  // TODO: add subgraph metadata fields.

  // Response fields.
  size_t bytes_written;
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

  [[nodiscard]]
  Type get_type() const { return type_; }

 private:
  Type type_;

  union {
    ReadMessage read_message;
    ExecuteMessage execute_message;
    WriteMessage write_message;
  } message_;
};

}  // namespace sics::graph::core::scheduler

// The following snippet helps the logger to format the MessageType enum.
template <>
struct ::fmt::formatter<sics::graph::core::scheduler::Message::Type> {
  typedef sics::graph::core::scheduler::Message::Type MessageType;
  using sics::graph::core::scheduler::Message::Type::kRead;
  using sics::graph::core::scheduler::Message::Type::kExecute;
  using sics::graph::core::scheduler::Message::Type::kWrite;

  constexpr auto parse(::fmt::format_parse_context& ctx)
      -> ::fmt::format_parse_context::iterator {
    return ctx.begin();
  }

  auto format(const MessageType& type, ::fmt::format_context& ctx) const
      -> fmt::format_context::iterator {
    switch (type) {
      case kRead:
        return fmt::format_to(ctx.out(), "ReadMessage");
      case kExecute:
        return fmt::format_to(ctx.out(), "ExecuteMessage");
      case kWrite:
        return fmt::format_to(ctx.out(), "WriteMessage");
    }
  }
};

#endif  // GRAPH_SYSTEMS_MESSAGE_H
