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
using EdgeIndex = core::common::EdgeIndex;
using FuncVertex = core::common::FuncVertex;
using FuncEdge = core::common::FuncEdge;
using FuncEdgeAndMutate = core::common::FuncEdgeAndMutate;
using FuncEdgeMutate = core::common::FuncEdgeMutate;

struct ReadMessage {
  ReadMessage() = default;
  ReadMessage(const ReadMessage& message) = default;
  // Request fields.
  core::common::GraphID graph_id = 0;
  core::common::VertexCount num_vertices = 0;
  bool changed = false;
  bool use_two_hop = false;
  core::data_structures::Serialized* serialized =
      nullptr;  // initialized in scheduler
  core::data_structures::Serializable* graph =
      nullptr;  // initialized in scheduler
  // Response fields.
  size_t bytes_read = 0;  // Use MB

  // Termination flag.
  bool terminated = false;
};

typedef enum {
  kDeserialize = 1,
  kCompute,
  kSerialize,
} ExecuteType;

enum MapType {
  kDefault = 0,
  kMapVertex = 1,
  kMapEdge = 2,
  kMapEdgeAndMutate = 3
};

struct ExecuteMessage {
  ExecuteMessage() = default;
  //  ~ExecuteMessage() { LOG_INFO("ExecuteMessage destructor called"); }
  //  ~ExecuteMessage() = default;

  // copy constructor
  //  ExecuteMessage(const ExecuteMessage& message) {
  //    graph_id = message.graph_id;
  //    serialized = message.serialized;
  //    execute_type = message.execute_type;
  //    graph = message.graph;
  //    map_type = message.map_type;
  //    switch (map_type) {
  //      case kMapVertex:
  //        func_vertex = message.func_vertex;
  //        break;
  //      case kMapEdge:
  //        func_edge = message.func_edge;
  //        break;
  //      case kMapEdgeAndMutate:
  //        func_edge_mutate = message.func_edge_mutate;
  //        break;
  //      default:
  //        break;
  //    }
  //    response_serializable = message.response_serializable;
  //    terminated = message.terminated;
  //  }

  // copy assignment operator
  //  ExecuteMessage& operator=(const ExecuteMessage& message) {
  //    if (this != &message) {
  //      graph_id = message.graph_id;
  //      serialized = message.serialized;
  //      execute_type = message.execute_type;
  //      graph = message.graph;
  //      map_type = message.map_type;
  //      switch (map_type) {
  //        case kMapVertex:
  //          func_vertex = message.func_vertex;
  //          break;
  //        case kMapEdge:
  //          func_edge = message.func_edge;
  //          break;
  //        case kMapEdgeAndMutate:
  //          func_edge_mutate = message.func_edge_mutate;
  //          break;
  //        default:
  //          break;
  //      }
  //      response_serializable = message.response_serializable;
  //      terminated = message.terminated;
  //    }
  //    return *this;
  //  }

  // Request fields.
  core::common::GraphID graph_id = 0;
  core::data_structures::Serialized* serialized = nullptr;
  ExecuteType execute_type = kCompute;
  MapType map_type = kDefault;
  // TODO: add subgraph metadata fields and API program objects.
  core::data_structures::Serializable* graph = nullptr;

  FuncVertex* func_vertex = nullptr;
  FuncEdge* func_edge = nullptr;
  FuncEdgeAndMutate* func_edge_mutate = nullptr;
  FuncEdgeMutate* func_edge_mutate_bool = nullptr;
  bool use_two_hop = false;

  // Response fields.
  core::data_structures::Serializable* response_serializable = nullptr;

  // Termination flag.
  bool terminated = false;
};

struct WriteMessage {
  WriteMessage() = default;
  // Request fields.
  core::common::GraphID graph_id = 0;
  core::data_structures::Serializable* graph = nullptr;
  core::data_structures::Serialized* serialized = nullptr;
  // TODO: add subgraph metadata fields.
  bool changed = false;
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
  ~Message() {}

  //  Message(const Message& message);

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
      default:
        return fmt::format_to(ctx.out(), "UnknownMessageType");
    }
  }
};

template <>
struct fmt::formatter<sics::graph::nvme::scheduler::MapType> {
  typedef sics::graph::nvme::scheduler::MapType MapType;

  constexpr auto parse(::fmt::format_parse_context& ctx)
      -> ::fmt::format_parse_context::iterator {
    return ctx.begin();
  }

  auto format(const MapType& type, ::fmt::format_context& ctx) const
      -> fmt::format_context::iterator {
    switch (type) {
      case MapType::kDefault:
        return fmt::format_to(ctx.out(), "Default");
      case MapType::kMapVertex:
        return fmt::format_to(ctx.out(), "MapVertex");
      case MapType::kMapEdge:
        return fmt::format_to(ctx.out(), "MapEdge");
      case MapType::kMapEdgeAndMutate:
        return fmt::format_to(ctx.out(), "MapEdgeAndMutate");
      default:
        return fmt::format_to(ctx.out(), "UnknownMapType");
    }
  }
};

#endif  // GRAPH_SYSTEMS_NVME_SCHEDULER_MESSAGE_H_
