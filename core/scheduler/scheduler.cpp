#include "scheduler.h"

#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::scheduler {

void Scheduler::Start() {
  thread_ = std::make_unique<std::thread>([this]() {
    bool running = true;
    // init round 0 loaded graph
    ReadMessage first_read_message;
    first_read_message.graph_id = GetNextReadGraphInCurrentRound();
    common::subgraph_limits--;
    message_hub_.get_reader_queue()->Push(first_read_message);

    while (running) {
      Message resp = message_hub_.GetResponse();

      if (resp.is_terminated()) {
        LOG_INFO("*** Scheduler is signaled termination ***");
        break;
      }
      switch (resp.get_type()) {
        case Message::Type::kRead: {
          ReadMessage read_response;
          resp.Get(&read_response);
          ReadMessageResponseAndExecute(read_response);
          break;
        }
        case Message::Type::kExecute: {
          ExecuteMessage execute_response;
          resp.Get(&execute_response);
          ExecuteMessageResponseAndWrite(execute_response);
          break;
        }
        case Message::Type::kWrite: {
          WriteMessage write_response;
          resp.Get(&write_response);
          if (!WriteMessageResponseAndCheckTerminate(write_response)) {
            running = false;
          }
          break;
        }
        default:
          break;
      }
    }
    LOG_INFO("*** Scheduler is signaled termination ***");
  });
}

// protected methods: (virtual)
// should be override by other scheduler

bool Scheduler::ReadMessageResponseAndExecute(const ReadMessage& read_resp) {
  // read finish, to execute the loaded graph
  graph_state_.SetGraphState(read_resp.graph_id,
                             GraphState::StorageStateType::Serialized);

  // read graph need deserialize first
  // check current round subgraph and send it to executer to Deserialization.
  if (graph_state_.GetSubgraphRound(read_resp.graph_id) == current_round_) {
    ExecuteMessage execute_message;
    execute_message.graph_id = read_resp.graph_id;
    execute_message.serialized = read_resp.response_serialized;
    std::unique_ptr<data_structures::Serializable> serializable_graph =
        CreateSerializableGraph(read_resp.graph_id);
    graph_state_.SetSubGraph(read_resp.graph_id, std::move(serializable_graph));
    execute_message.graph = graph_state_.GetSubgraph(read_resp.graph_id);
    execute_message.execute_type = ExecuteType::kDeserialize;
    message_hub_.get_executor_queue()->Push(execute_message);
  } else {
    graph_state_.SetSubgraphSerialized(
        read_resp.graph_id, std::unique_ptr<data_structures::Serialized>(
                                read_resp.response_serialized));
  }
  TryReadNextGraph();
  return true;
}

bool Scheduler::ExecuteMessageResponseAndWrite(
    const ExecuteMessage& execute_resp) {
  // execute finish, to write back the graph
  switch (execute_resp.execute_type) {
    case ExecuteType::kDeserialize: {
      ExecuteMessage execute_message;
      execute_message.graph_id = execute_resp.graph_id;
      execute_message.graph = execute_resp.response_serializable;
      if (current_round_ == 0) {
        execute_message.execute_type = ExecuteType::kPEval;
      } else {
        execute_message.execute_type = ExecuteType::kIncEval;
      }
      message_hub_.get_executor_queue()->Push(execute_message);
      break;
    }
    case ExecuteType::kPEval:
    case ExecuteType::kIncEval: {
      // check if current round finish
      if (IsCurrentRoundFinish()) {
        if (IsSystemStop()) {
          // read graph in next round
        } else {
          return false;
        }
      } else {
        // write back to disk or save in memory
        // TODO: check if graph can stay in memory
        if (false) {
          // stay in memory with StorageStateType::Deserialized
        } else {
          // write back to disk
          ExecuteMessage execute_message(execute_resp);
          execute_message.execute_type = ExecuteType::kSerialize;
          message_hub_.get_executor_queue()->Push(execute_resp);
        }
      }
      // check border vertex and dependency matrix, mark active subgraph in
      // next round
      // TODO: check border vertex and dependency matrix
      break;
    }
    case ExecuteType::kSerialize: {
      WriteMessage write_message;
      write_message.graph_id = execute_resp.graph_id;
      write_message.serialized = execute_resp.serialized;
      message_hub_.get_writer_queue()->Push(write_message);

      TryReadNextGraph();
      break;
    }
    default:
      LOG_WARN("Executer response show it doing nothing!");
      break;
  }

  TryReadNextGraph();
  return true;
}

bool Scheduler::WriteMessageResponseAndCheckTerminate(
    const WriteMessage& write_resp) {
  graph_state_.SetGraphState(write_resp.graph_id,
                             GraphState::StorageStateType::OnDisk);
  TryReadNextGraph(true);
  return true;
}

// private methods:
bool Scheduler::TryReadNextGraph(bool sync) {
  if (common::subgraph_limits > 0) {
    auto next_graph_id = GetNextReadGraphInCurrentRound();
    ReadMessage read_message;
    if (next_graph_id != INVALID_GRAPH_ID) {
      read_message.graph_id = next_graph_id;
      message_hub_.get_reader_queue()->Push(read_message);
    } else {
      // check next round graph which can be read, if not just skip
      if (sync) {
        current_round_++;
        graph_state_.SyncRoundState();
      }
      auto next_gid_next_round = GetNextReadGraphInNextRound();
      if (next_gid_next_round != INVALID_GRAPH_ID) {
        read_message.graph_id = next_gid_next_round;
        message_hub_.get_reader_queue()->Push(read_message);
      } else {
        // no graph can be read, terminate system
        return false;
      }
    }
  }
  return true;
}

std::unique_ptr<data_structures::Serializable>
Scheduler::CreateSerializableGraph(common::GraphID graph_id) {
  if (common::configs.vertex_type == common::VertexDataType::kVertexDataTypeUInt32) {
    return std::make_unique<data_structures::graph::MutableCSRGraphUInt32>(
        graph_metadata_info_.GetSubgraphMetadataRef(graph_id));

  } else {
    return std::make_unique<data_structures::graph::MutableCSRGraphUInt16>(
        graph_metadata_info_.GetSubgraphMetadataRef(graph_id));
  }
}

common::GraphID Scheduler::GetNextReadGraphInCurrentRound() const {
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) ==
            GraphState::StorageStateType::OnDisk) {
      return gid;
    }
  }
  return INVALID_GRAPH_ID;
}

common::GraphID Scheduler::GetNextExecuteGraph() const {
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) ==
            GraphState::StorageStateType::Deserialized) {
      return gid;
    }
  }
  return INVALID_GRAPH_ID;
}

common::GraphID Scheduler::GetNextReadGraphInNextRound() const {
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.next_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) ==
            GraphState::StorageStateType::OnDisk) {
      return gid;
    }
  }
  return INVALID_GRAPH_ID;
}

}  // namespace sics::graph::core::scheduler