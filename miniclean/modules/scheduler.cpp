#include "miniclean/modules/scheduler.h"

namespace sics::graph::miniclean::modules {

using GraphID = sics::graph::miniclean::common::GraphID;

Scheduler::Scheduler(const std::string& data_home,
                     const std::string& graph_home)
    : io_manager_(data_home, graph_home) {
  // Initialize config.
  available_memory_size_ = MiniCleanConfig::Get()->memory_size_;
}

void Scheduler::Start() {
  thread_ = std::make_unique<std::thread>([this]() {
    LOG_INFO(" ========== Launched MiniClean scheduler. ========== ");
    LOGF_INFO("Available memory size: {} MB", available_memory_size_);
    ReadMessage first_read_message;
    first_read_message.graph_id = GetNextReadGraphInCurrentRound();
    first_read_message.response_serialized =
        io_manager_.NewSerializedSubgraph(first_read_message.graph_id);
    io_manager_.SetSubgraphState(first_read_message.graph_id,
                                 GraphStateType::kReading);
    message_hub_.SendRequest(first_read_message);
    LOG_INFO("Initialize the message queues with a read request.");

    bool running = true;
    while (running) {
      Message resp = message_hub_.GetResponse();

      if (resp.is_terminated()) {
        LOG_INFO("*** Scheduler is signaled termination. ***");
        break;
      }
      switch (resp.get_type()) {
        case Message::Type::kRead: {
          ReadMessage read_response;
          resp.Get(&read_response);
          if (!ReadMessageResponseAndExecute(read_response)) {
            LOG_INFO("Exited from ReadMessageResponseAndExecute.");
            running = false;
          }
          break;
        }
        case Message::Type::kExecute: {
          ExecuteMessage execute_response;
          resp.Get(&execute_response);
          if (ExecuteMessageResponseAndWrite(execute_response)) {
            LOG_INFO("Exited from ExecuteMessageResponseAndWrite.");
            running = false;
          }
          break;
        }
        case Message::Type::kWrite: {
          /* code */
          break;
        }
        default:
          break;
      }
    }
    LOGF_INFO(" ========== Scheduler exited at round {}. ========== ",
              current_round_);
  });
}

bool Scheduler::ReadMessageResponseAndExecute(const ReadMessage& read_resp) {
  io_manager_.SetSubgraphState(read_resp.graph_id, GraphStateType::kSerialized);
  // For BSP model, check the graph round.
  if (io_manager_.GetSubgraphRound(read_resp.graph_id) != current_round_) {
    LOGF_FATAL("The round of the graph {} is {}, but the current round is {}.",
               read_resp.graph_id,
               io_manager_.GetSubgraphRound(read_resp.graph_id),
               current_round_);
  }

  // The graph has been read and becomes a serialized data in the memory.
  io_manager_.SetSubgraphState(read_resp.graph_id, GraphStateType::kSerialized);
  // Send deserialize request.
  ExecuteMessage execute_message;
  execute_message.graph_id = read_resp.graph_id;
  execute_message.serialized = read_resp.response_serialized;
  io_manager_.NewSubgraph(execute_message.graph_id);
  execute_message.graph = io_manager_.GetSubgraph(execute_message.graph_id);
  execute_message.execute_type = sics::graph::miniclean::messages::kDeserialize;
  message_hub_.SendRequest(execute_message);
  LOGF_INFO("Loaded serialized data and sent deserialize request for graph {}.",
            execute_message.graph_id);
  return true;
}

bool Scheduler::ExecuteMessageResponseAndWrite(
    const ExecuteMessage& execute_resp) {
  switch (execute_resp.execute_type) {
    case sics::graph::miniclean::messages::kDeserialize: {
      io_manager_.SetSubgraphState(execute_resp.graph_id,
                                   GraphStateType::kDeserialized);
      // TODO (bai-wenchao): Construct and send next request.
      break;
    }
    default:
      break;
  }
}

// TODO (bai-wenchao): Decide which graph to read next.
GraphID Scheduler::GetNextReadGraphInCurrentRound() {
  for (GraphID gid = 0; gid < io_manager_.GetNumSubgraphs(); ++gid) {
    if (io_manager_.GetSubgraphState(gid) == kOnDisk ||
        io_manager_.IsCurrentPendingSubgraph(gid)) {
      return gid;
    }
    // TODO (bai-wenchao): this macro is defined in `core/common/types.h` not in
    // `miniclean/common/types.h`.
    // For the current usage, it's OK because the definitions of GraphID are the
    // same in both files. However, if one of `GraphID` changes, we should set a
    // new invalid value for `GraphID` under `miniclean`.
    return INVALID_GRAPH_ID;
  }
}
}  // namespace sics::graph::miniclean::modules