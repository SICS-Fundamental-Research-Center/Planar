#include "scheduler.h"

namespace sics::graph::core::scheduler {

void Scheduler::Start() {
  thread_ = std::make_unique<std::thread>([this]() {
    LOGF_INFO(" ============ Current Round: {} ============ ", current_round_);
    bool running = true;
    // init round 0 loaded graph
    ReadMessage first_read_message;
    first_read_message.graph_id = GetNextReadGraphInCurrentRound();
    first_read_message.num_vertices =
        graph_metadata_info_.GetSubgraphNumVertices(
            first_read_message.graph_id);
    first_read_message.response_serialized =
        graph_state_.NewSerializedMutableCSRGraph(first_read_message.graph_id);
    first_read_message.round = 0;
    graph_state_.subgraph_limits_--;
    auto read_size =
        graph_metadata_info_.GetSubgraphSize(first_read_message.graph_id);
    if (memory_left_size_ - read_size < 0) {
      LOG_FATAL("read size is too large, memory is not enough!");
    }
    memory_left_size_ -= read_size;
    graph_state_.SetOnDiskToReading(first_read_message.graph_id);
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
          if (!ExecuteMessageResponseAndWrite(execute_response)) {
            running = false;
          }
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
  graph_state_.SetReadingToSerialized(read_resp.graph_id);

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
    // TODO: do something when load next round subgraph
  }
  TryReadNextGraph();
  return true;
}

bool Scheduler::ExecuteMessageResponseAndWrite(
    const ExecuteMessage& execute_resp) {
  // execute finish, to write back the graph
  switch (execute_resp.execute_type) {
    case ExecuteType::kDeserialize: {
      graph_state_.SetSerializedToDeserialized(execute_resp.graph_id);

      ExecuteMessage execute_message;
      execute_message.graph_id = execute_resp.graph_id;
      execute_message.graph = execute_resp.response_serializable;
      if (current_round_ == 0) {
        execute_message.execute_type = ExecuteType::kPEval;
      } else {
        execute_message.execute_type = ExecuteType::kIncEval;
      }
      SetRuntimeGraph(execute_message.graph_id);
      execute_message.app = app_;
      message_hub_.get_executor_queue()->Push(execute_message);
      break;
    }
    case ExecuteType::kPEval:
    case ExecuteType::kIncEval: {
      graph_state_.SetDeserializedToComputed(execute_resp.graph_id);

      // TODO: decide a subgraph if it stays in memory
      ExecuteMessage execute_message(execute_resp);
      execute_message.graph_id = execute_resp.graph_id;
      execute_message.execute_type = ExecuteType::kSerialize;
      message_hub_.get_executor_queue()->Push(execute_message);
      break;
    }
    case ExecuteType::kSerialize: {
      graph_state_.SetComputedToSerialized(execute_resp.graph_id);
      graph_state_.ReleaseSubgraph(execute_resp.graph_id);
      // Check if current round finish.
      if (IsCurrentRoundFinish()) {
        if (IsSystemStop()) {
          WriteMessage write_message;
          write_message.graph_id = execute_resp.graph_id;
          write_message.serialized = execute_resp.serialized;
          message_hub_.get_writer_queue()->Push(write_message);

          // Release other graphs in memory, write back to disk.
          ReleaseAllGraph();
          return false;
        } else {
          // This sync maybe replaced by borderVertex check.
          auto active = update_store_->GetActiveCount();
          graph_state_.SyncCurrentRoundPending();
          update_store_->Sync();
          current_round_++;
          LOGF_INFO(
              " ============ Current Round: {}, Active vertex left: {} "
              "============ ",
              current_round_, active);

          WriteMessage write_message;
          write_message.graph_id = execute_resp.graph_id;
          write_message.serialized = execute_resp.serialized;
          message_hub_.get_writer_queue()->Push(write_message);
        }
      } else {
        // Write back to disk or save in memory.
        // TODO: Check if graph can stay in memory.
        if (false) {
          // stay in memory with StorageStateType::Deserialized
        } else {
          // write back to disk
          WriteMessage write_message;
          write_message.graph_id = execute_resp.graph_id;
          write_message.serialized = execute_resp.serialized;
          message_hub_.get_writer_queue()->Push(write_message);
        }
      }
      // Check border vertex and dependency matrix, mark active subgraph in
      // next round.
      // Now, load all subgraphs in next round.
      // TODO: Check border vertex and dependency matrix.
      break;
    }
    default:
      LOG_WARN("Executor response show it doing nothing!");
      break;
  }

  // Where to use this.
  TryReadNextGraph();
  return true;
}

bool Scheduler::WriteMessageResponseAndCheckTerminate(
    const WriteMessage& write_resp) {
  // Update subgraph state.
  graph_state_.subgraph_limits_++;
  graph_state_.SetSerializedToOnDisk(write_resp.graph_id);
  graph_state_.ReleaseSubgraphSerialized(write_resp.graph_id);
  auto write_size = graph_metadata_info_.GetSubgraphSize(write_resp.graph_id);
  memory_left_size_ += write_size;
  // Read next subgraph if permitted
  TryReadNextGraph();
  return true;
}

// private methods:

// try to read a graph from disk into memory if memory_limit is permitted
bool Scheduler::TryReadNextGraph(bool sync) {
  //  if (graph_state_.subgraph_limits_ > 0) {
  if (memory_left_size_ > 0) {
    auto next_graph_id = GetNextReadGraphInCurrentRound();
    ReadMessage read_message;
    if (next_graph_id != INVALID_GRAPH_ID) {
      auto read_size = graph_metadata_info_.GetSubgraphSize(next_graph_id);
      if (memory_left_size_ - read_size < 0) {
        // Memory is not enough, return.
        return false;
      }
      memory_left_size_ -= read_size;
      read_message.graph_id = next_graph_id;
      read_message.num_vertices =
          graph_metadata_info_.GetSubgraphNumVertices(next_graph_id);
      read_message.round = graph_state_.GetSubgraphRound(next_graph_id);
      read_message.response_serialized =
          graph_state_.NewSerializedMutableCSRGraph(next_graph_id);
      graph_state_.SetOnDiskToReading(next_graph_id);
      message_hub_.get_reader_queue()->Push(read_message);
    } else {
      // check next round graph which can be read, if not just skip
      if (sync) {
        current_round_++;
      }
      auto next_gid_next_round = GetNextReadGraphInNextRound();
      if (next_gid_next_round != INVALID_GRAPH_ID) {
        auto read_size =
            graph_metadata_info_.GetSubgraphSize(next_gid_next_round);
        if (memory_left_size_ - read_size < 0) {
          return false;
        }
        memory_left_size_ -= read_size;
        read_message.graph_id = next_gid_next_round;
        read_message.num_vertices =
            graph_metadata_info_.GetSubgraphNumVertices(read_message.graph_id);
        read_message.round =
            graph_state_.GetSubgraphRound(read_message.graph_id);
        read_message.response_serialized =
            graph_state_.NewSerializedMutableCSRGraph(next_gid_next_round);
        message_hub_.get_reader_queue()->Push(read_message);
      } else {
        // no graph can be read, terminate system
        return false;
      }
    }
    graph_state_.subgraph_limits_--;
  }
  return true;
}

std::unique_ptr<data_structures::Serializable>
Scheduler::CreateSerializableGraph(common::GraphID graph_id) {
  if (common::Configurations::Get()->vertex_data_type ==
      common::VertexDataType::kVertexDataTypeUInt32) {
    return std::make_unique<MutableCSRGraphUInt32>(
        graph_metadata_info_.GetSubgraphMetadataPtr(graph_id),
        graph_metadata_info_.get_num_vertices());

  } else {
    return std::make_unique<MutableCSRGraphUInt16>(
        graph_metadata_info_.GetSubgraphMetadataPtr(graph_id),
        graph_metadata_info_.get_num_vertices());
  }
}

// release all graphs in memory. not write back to disk. just release memory.
void Scheduler::ReleaseAllGraph() {
  for (int i = 0; i < graph_state_.num_subgraphs_; i++) {
    //    graph_state_.
  }
}

void Scheduler::SetRuntimeGraph(common::GraphID gid) {
  if (common::Configurations::Get()->vertex_data_type ==
      common::VertexDataType::kVertexDataTypeUInt32) {
    auto app = dynamic_cast<apis::PlanarAppBase<MutableCSRGraphUInt32>*>(app_);
    app->SetRuntimeGraph(
        dynamic_cast<MutableCSRGraphUInt32*>(graph_state_.GetSubgraph(gid)));
  } else {
    auto app = dynamic_cast<apis::PlanarAppBase<MutableCSRGraphUInt16>*>(app_);
    app->SetRuntimeGraph(
        dynamic_cast<MutableCSRGraphUInt16*>(graph_state_.GetSubgraph(gid)));
  }
}

common::GraphID Scheduler::GetNextReadGraphInCurrentRound() const {
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) == GraphState::OnDisk) {
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

bool Scheduler::IsCurrentRoundFinish() const {
  for (int i = 0; i < graph_metadata_info_.get_num_subgraphs(); i++) {
    if (graph_state_.current_round_pending_.at(i)) {
      return false;
    }
  }
  return true;
}

bool Scheduler::IsSystemStop() const {
  return IsCurrentRoundFinish() && !update_store_->IsActive();
}

}  // namespace sics::graph::core::scheduler