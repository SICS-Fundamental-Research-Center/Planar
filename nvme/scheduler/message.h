#ifndef GRAPH_SYSTEMS_NVME_SCHEDULER_MESSAGE_H_
#define GRAPH_SYSTEMS_NVME_SCHEDULER_MESSAGE_H_

#include <string>

#include "core/common/blocking_queue.h"
#include "core/common/types.h"
#include "core/data_structures/serializable.h"
#include "core/data_structures/serialized.h"
#include "core/util/logging.h"

namespace sics::graph::nvme::scheduler {

using VertexID = core::common::VertexID;

struct ReadMessage {
  ReadMessage() = default;
  ReadMessage(const ReadMessage& message) = default;
  // Request fields.
  core::common::GraphID graph_id;
  core::common::VertexCount num_vertices;
  int round;

  // Response fields.
  core::data_structures::Serialized*
      response_serialized;  // initialized in loader

  // Termination flag.
  bool terminated = false;
};

typedef enum {
  kDeserialize = 1,
  kCompute,
  kMapVertex,
  kMapEdge,
  kMapEdgeAndMutate,
  kSerialize,
} ExecuteType;

struct ExecuteMessage {
  ExecuteMessage() = default;
  ~ExecuteMessage() { func_vertex.~function(); }

  // copy constructor
  ExecuteMessage(const ExecuteMessage& message) {
    graph_id = message.graph_id;
    serialized = message.serialized;
    execute_type = message.execute_type;
    graph = message.graph;
    func_vertex = message.func_vertex;
    response_serializable = message.response_serializable;
    terminated = message.terminated;
  }

  // copy assignment operator
  ExecuteMessage& operator=(const ExecuteMessage& message) {
    if (this != &message) {
      graph_id = message.graph_id;
      serialized = message.serialized;
      execute_type = message.execute_type;
      graph = message.graph;
      func_vertex = message.func_vertex;
      response_serializable = message.response_serializable;
      terminated = message.terminated;
    }
    return *this;
  }

  // Request fields.
  core::common::GraphID graph_id;
  core::data_structures::Serialized* serialized;
  ExecuteType execute_type = kCompute;
  // TODO: add subgraph metadata fields and API program objects.
  core::data_structures::Serializable* graph;

  std::function<void(VertexID)> func_vertex;
  //  std::function<void(VertexID, VertexID)> func_edge;

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
  ~Message();

  Message(const Message& message);

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
        return this->read_message.terminated;
      case kExecute:
        return this->execute_message.terminated;
      case kWrite:
        return this->write_message.terminated;
      default:
        return false;
    }
  }

 private:
  Type type_;

  union {
    ReadMessage read_message;
    ExecuteMessage execute_message;
    WriteMessage write_message;
    //    explicit Messages(const ReadMessage& message) { read_message =
    //    message; }; explicit Messages(const ExecuteMessage& message) {
    //      execute_message = message;
    //    };
    //    explicit Messages(const WriteMessage& message) { write_message =
    //    message; };
    // copy constructor

    //    ~Messages() { execute_message.~ExecuteMessage(); }
  };
};

}  // namespace sics::graph::nvme::scheduler

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
