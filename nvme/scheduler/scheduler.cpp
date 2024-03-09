#include "scheduler.h"

namespace sics::graph::nvme::scheduler {

void PramScheduler::Start() {
  thread_ = std::make_unique<std::thread>([this]() {
    LOG_INFO("*** PramScheduler starts ***");
    bool running = true;
    while (running) {
      Message resp = message_hub_.GetResponse();

      if (resp.is_terminated()) {
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
          WriteMessageResponseAndCheckTerminate(write_response);
          break;
        }
        default:
          break;
      }
    }
    // LOG_INFO("*** PramScheduler is signaled termination ***");
  });
}

bool PramScheduler::ReadMessageResponseAndExecute(
    const ReadMessage& read_resp) {
  // Read finish, to execute the loaded graph.
  graph_state_.SetGraphState(read_resp.graph_id, GraphState::Deserialized);
  // If executor is not running, execute the blocks.
  if (!is_executor_running_) {
    SendExecuteMessage(read_resp.graph_id);
  }
  TryReadNextGraph();
  return true;
}

bool PramScheduler::ExecuteMessageResponseAndWrite(
    const ExecuteMessage& execute_resp) {
  is_executor_running_ = false;

  // Require map type of current graph.
  if (current_Map_type_ == kDefault) {
    current_Map_type_ = execute_resp.map_type;
    switch (current_Map_type_) {
      case MapType::kMapVertex:
        func_vertex_ = execute_resp.func_vertex;
        break;
      case MapType::kMapEdge:
        func_edge_ = execute_resp.func_edge;
        break;
      case MapType::kMapEdgeAndMutate:
        func_edge_mutate_bool_ = execute_resp.func_edge_mutate_bool;
        break;
      default:
        LOG_ERROR("Map type is not supported!");
        break;
    }
    // Read first block.
    if (in_memory_) {
      // If in-memory mode, first round read, than keep the block in memory.
      ExecuteNextGraphInMemory();
    } else {
      TryReadNextGraph();
    }
  } else {
    //    if (in_memory_) {
    // Still keep deserialized state.
    graph_state_.SetCurrentRoundPendingFinish(execute_resp.graph_id);
    if (!in_memory_) {
      SendWriteMessage(execute_resp.graph_id);  // Write back to disk.
    }
    if (IsCurrentRoundFinish()) {
      graph_state_.ResetCurrentRoundPending();
      update_store_->Sync();
      if (current_Map_type_ == MapType::kMapEdgeAndMutate) {
        graph_metadata_info_.UpdateOutEdgeNumInBLockMode();
        graph_state_.SetBlockMutated(execute_resp.graph_id);
      }
      //              LOGF_INFO(" Current MapType: {}, Step: {}",
      //              current_Map_type_,
      //                        step_);
      step_++;
      ResetMapFunction();
      // Return current map function scheduling.
      UnlockAndReleaseResult();
    } else {
      ExecuteNextGraphInMemory();
    }
    //    } else {
    //      ExecuteNextGraphInMemory();
    //    }
  }
  return true;
}

bool PramScheduler::WriteMessageResponseAndCheckTerminate(
    const WriteMessage& write_resp) {
  // Update subgraph state.
  graph_state_.SetGraphState(write_resp.graph_id, GraphState::OnDisk);
  auto write_size = graph_metadata_info_.GetBlockSize(write_resp.graph_id);
  //  LOGF_INFO(
  //      "Release subgraph: {}, size: {}. *** Memory size now: {}, after: {} "
  //      "***",
  //      write_resp.graph_id, write_size, memory_left_size_,
  //      memory_left_size_ + write_size);
  if (use_limits_) {
    limits_++;
    //    LOGF_INFO("Write back one graph. now limits is {}", limits_);
  } else {
    memory_left_size_ += write_size;
    graph_metadata_info_.UpdateBlockSize(write_resp.graph_id);
  }
  // Read next subgraph if permitted.
  if (release_) {
    if (IsSchedulerStop()) UnlockAndReleaseResult();
  } else {
    if (current_Map_type_ != MapType::kDefault) {
      TryReadNextGraph();
    }
  }
  return true;
}

// private methods:

// try to read a graph from disk into memory if memory_limit is permitted
bool PramScheduler::TryReadNextGraph(bool sync) {
  auto load_graph_id = GetNextReadGraphInCurrentRound();
  if (load_graph_id == INVALID_GRAPH_ID) return false;

  // group_mode use the same logic
  if (use_limits_) {
    if (limits_ <= 0) return false;
    limits_--;
    // LOGF_INFO("Read on graph {}. now limits is {}", load_graph_id, limits_);
  } else {
    auto read_size = graph_metadata_info_.GetBlockSize(load_graph_id);
    if (memory_left_size_ < read_size) return false;
    //    LOGF_INFO(
    //        "To read subgraph {}. *** Memory size now: {}, after read: {} "
    //        "***",
    //        load_graph_id, memory_left_size_, memory_left_size_ - read_size);
    memory_left_size_ -= read_size;
  }

  // condition satisfied
  ReadMessage read_message;
  read_message.graph_id = load_graph_id;
  read_message.num_vertices =
      graph_metadata_info_.GetBlockNumVertices(load_graph_id);
  //  read_message.serialized = CreateSerialized(load_graph_id);
  read_message.graph = CreateSerializableGraph(load_graph_id);
  read_message.changed = graph_state_.IsBlockMutated(load_graph_id);
  graph_state_.SetOnDiskToReading(load_graph_id);
  message_hub_.get_reader_queue()->Push(read_message);
  return true;
}

core::data_structures::Serializable* PramScheduler::CreateSerializableGraph(
    GraphID graph_id) {
  if (core::common::Configurations::Get()->vertex_data_type ==
      core::common::VertexDataType::kVertexDataTypeUInt32) {
    std::unique_ptr<core::data_structures::Serializable> serializable_graph =
        std::make_unique<BlockCSRGraphUInt32>(
            graph_metadata_info_.GetBlockMetadataPtr(graph_id));
    graph_state_.SetSubGraph(graph_id, std::move(serializable_graph));
  } else {
    std::unique_ptr<core::data_structures::Serializable> serializable_graph =
        std::make_unique<BlockCSRGraphUInt16>(
            graph_metadata_info_.GetBlockMetadataPtr(graph_id));
    graph_state_.SetSubGraph(graph_id, std::move(serializable_graph));
  }
  return graph_state_.GetSubgraph(graph_id);
}

core::data_structures::Serialized* PramScheduler::CreateSerialized(
    GraphID graph_id) {
  return graph_state_.NewSerializedBlockGraph(graph_id);
}

// release all graphs in memory. not write back to disk. just release memory.
void PramScheduler::ReleaseAllGraph() {
  for (int i = 0; i < graph_state_.num_blocks_; i++) {
    if (graph_state_.subgraph_storage_state_.at(i) == GraphState::Serialized)
      graph_state_.ReleaseSubgraphSerialized(i);
  }
}

void PramScheduler::SetAppRuntimeGraph(GraphID gid) {
  if (group_mode_) {
  } else {
    if (core::common::Configurations::Get()->vertex_data_type ==
        core::common::VertexDataType::kVertexDataTypeUInt32) {
    } else {
    }
  }
}

void PramScheduler::SetGlobalVertexData(GraphID bid) {
  auto block = (BlockCSRGraphUInt32*)graph_state_.GetSubgraph(bid);
  block->SetGlobalVertexData(update_store_);
}

core::common::GraphID PramScheduler::GetNextReadGraphInCurrentRound() const {
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) == GraphState::OnDisk) {
      return gid;
    }
  }
  return INVALID_GRAPH_ID;
}

core::common::GraphID PramScheduler::GetNextExecuteGraph() const {
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) ==
            GraphState::StorageStateType::Serialized) {
      return gid;
    }
  }
  return INVALID_GRAPH_ID;
}

core::common::GraphID PramScheduler::GetNextExecuteGraphInMemory() const {
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) ==
            GraphState::StorageStateType::Deserialized) {
      return gid;
    }
  }
  return INVALID_GRAPH_ID;
}

void PramScheduler::GetNextExecuteGroupGraphs() {
  int count = 0;
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) ==
            GraphState::StorageStateType::Serialized) {
      group_graphs_.push_back(gid);
      count++;
      // check if "group num" graphs have been added.
      if (count == group_num_) {
        break;
      }
    }
  }
  LOGF_INFO("group graphs num: {}", group_num_);
}

core::common::GraphID PramScheduler::GetNextReadGraphInNextRound() const {
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.subgraph_storage_state_.at(gid) ==
        GraphState::StorageStateType::OnDisk) {
      return gid;
    }
  }
  return INVALID_GRAPH_ID;
}

size_t PramScheduler::GetLeftPendingGraphNums() const {
  size_t res = 0;
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid)) {
      res++;
    }
  }
  return res;
}

bool PramScheduler::IsCurrentRoundFinish() const {
  for (int i = 0; i < graph_metadata_info_.get_num_subgraphs(); i++) {
    if (graph_state_.current_round_pending_.at(i)) {
      return false;
    }
  }
  return true;
}

bool PramScheduler::IsSchedulerStop() const {
  for (int i = 0; i < graph_metadata_info_.get_num_subgraphs(); i++) {
    if (graph_state_.subgraph_storage_state_.at(i) !=
        GraphState::StorageStateType::OnDisk) {
      return false;
    }
  }
  return true;
}

void PramScheduler::RunMapExecute(ExecuteMessage execute_msg) {
  message_hub_.get_response_queue()->Push(scheduler::Message(execute_msg));
}

void PramScheduler::SetExecuteMessageMapFunction(ExecuteMessage* message) {
  switch (current_Map_type_) {
    case MapType::kMapVertex: {
      message->map_type = MapType::kMapVertex;
      message->func_vertex = func_vertex_;
      break;
    }
    case MapType::kMapEdge: {
      message->map_type = MapType::kMapEdge;
      message->func_edge = func_edge_;
      break;
    }
    case MapType::kMapEdgeAndMutate: {
      message->map_type = MapType::kMapEdgeAndMutate;
      message->func_edge_mutate_bool = func_edge_mutate_bool_;
      break;
    }
    default:
      LOG_ERROR("Map type is not supported!");
      break;
  }
}

void PramScheduler::ResetMapFunction() {
  current_Map_type_ = MapType::kDefault;
  func_vertex_ = nullptr;
  func_edge_ = nullptr;
  func_edge_mutate_bool_ = nullptr;
}

void PramScheduler::UnlockAndReleaseResult() {
  std::lock_guard<std::mutex> lock(pram_mtx_);
  pram_ready_ = true;
  pram_cv_.notify_all();
}

void PramScheduler::CheckMapFunctionFinish() {
  if (current_Map_type_ != MapType::kDefault)
    LOGF_FATAL("Map type error when return from scheduler!", current_Map_type_);
  UnlockAndReleaseResult();
}

void PramScheduler::SendWriteMessage(GraphID bid) {
  WriteMessage write_message;
  write_message.graph_id = bid;
  write_message.graph = graph_state_.GetSubgraph(bid);
  write_message.changed = graph_state_.IsBlockMutated(bid);
  graph_state_.SetGraphState(bid, GraphState::StorageStateType::Writing);
  message_hub_.get_writer_queue()->Push(write_message);
}

bool PramScheduler::ReleaseInMemoryGraph() {
  bool flag = false;
  for (int i = 0; i < graph_metadata_info_.get_num_subgraphs(); i++) {
    if (graph_state_.GetSubgraphState(i) == GraphState::Deserialized) {
      LOGF_INFO("Release block: {} from memory", i);
      SendWriteMessage(i);
      flag = true;
    }
  }
  return flag;
}

void PramScheduler::SendExecuteMessage(core::common::GraphID bid) {
  ExecuteMessage execute_message;
  execute_message.graph_id = bid;
  execute_message.graph = graph_state_.GetSubgraph(bid);
  execute_message.execute_type = ExecuteType::kCompute;
  execute_message.map_type = current_Map_type_;
  SetExecuteMessageMapFunction(&execute_message);
  SetGlobalVertexData(bid);
  current_bid_ = bid;
  is_executor_running_ = true;
  message_hub_.get_executor_queue()->Push(execute_message);
}

void PramScheduler::ExecuteNextGraphInMemory() {
  auto next_execute_gid = GetNextExecuteGraphInMemory();
  if (next_execute_gid == INVALID_GRAPH_ID) {
    TryReadNextGraph();
    return;
  }
  SendExecuteMessage(next_execute_gid);
}

}  // namespace sics::graph::nvme::scheduler
