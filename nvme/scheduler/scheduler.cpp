#include "scheduler.h"

namespace sics::graph::nvme::scheduler {

void PramScheduler::Start() {
  thread_ = std::make_unique<std::thread>([this]() {
    LOG_INFO("*** PramScheduler starts ***");
    auto update_store_size = update_store_->GetMemorySize();
    //    memory_left_size_ -= update_store_size;
    LOGF_INFO("global memory size: {} MB", memory_left_size_);
    bool running = true;
    // init round 0 loaded graph
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
    LOG_INFO("*** PramScheduler is signaled termination ***");
  });
}

// protected methods: (virtual)
// should be override by other scheduler

bool PramScheduler::ReadMessageResponseAndExecute(
    const ReadMessage& read_resp) {
  // read finish, to execute the loaded graph
  graph_state_.SetReadingToSerialized(read_resp.graph_id);

  // read graph need deserialize first
  // check current round subgraph and send it to executer to Deserialization.
  if (group_mode_) {
    have_read_graphs_++;
    LOGF_INFO("already load graph number: {}", have_read_graphs_);
    if (!is_executor_running_) {
      if (have_read_graphs_ >= group_num_) {
        GetNextExecuteGroupGraphs();
        is_executor_running_ = true;
        for (int i = 0; i < group_num_; i++) {
          auto gid = group_graphs_.at(i);
          ExecuteMessage execute_message;
          execute_message.graph_id = gid;
          execute_message.serialized = graph_state_.GetSubgraphSerialized(gid);
          CreateSerializableGraph(gid);
          execute_message.graph = graph_state_.GetSubgraph(gid);
          execute_message.execute_type = ExecuteType::kDeserialize;
          LOGF_INFO("GROUP MODE: deserialize and execute the read graph {}",
                    gid);
          message_hub_.get_executor_queue()->Push(execute_message);
        }
      }
    }
  } else {
    if (!is_executor_running_) {
      ExecuteMessage execute_message;
      execute_message.graph_id = read_resp.graph_id;
      execute_message.serialized = read_resp.response_serialized;
      CreateSerializableGraph(read_resp.graph_id);
      execute_message.graph = graph_state_.GetSubgraph(read_resp.graph_id);
      execute_message.execute_type = ExecuteType::kDeserialize;
      is_executor_running_ = true;
      //      LOGF_INFO("read finished, deserialize and execute the read graph
      //      {}",
      //                read_resp.graph_id);
      message_hub_.get_executor_queue()->Push(execute_message);
    } else {
      // When executor is running, do nothing, wait for executor finish.
      //      LOG_INFO("read finished, but executor is running, wait for
      //      executor");
    }
  }
  TryReadNextGraph();
  return true;
}

bool PramScheduler::ExecuteMessageResponseAndWrite(
    const ExecuteMessage& execute_resp) {
  // Execute finish, decide next executor work.
  //  is_executor_running_ = false;
  switch (execute_resp.execute_type) {
    case ExecuteType::kDeserialize: {
      graph_state_.SetSerializedToDeserialized(execute_resp.graph_id);
      if (group_mode_) {
        group_deserialized_num_++;
        if (group_deserialized_num_ == group_num_) {
          ExecuteMessage execute_message;
          execute_message.graph_id = group_graphs_.at(0);
          execute_message.graph = group_serializable_graph_.get();
          execute_message.execute_type = kCompute;
          execute_message.map_type = current_Map_type_;
          SetGlobalVertexData(group_graphs_.at(0));
          message_hub_.get_executor_queue()->Push(execute_message);
        }
      } else {
        ExecuteMessage execute_message;
        execute_message.graph_id = execute_resp.graph_id;
        execute_message.graph = execute_resp.response_serializable;
        execute_message.execute_type = kCompute;
        execute_message.map_type = current_Map_type_;
        SetExecuteMessageMapFunction(&execute_message);
        SetGlobalVertexData(execute_message.graph_id);
        current_bid_ = execute_resp.graph_id;
        message_hub_.get_executor_queue()->Push(execute_message);
      }
      break;
    }
    case ExecuteType::kCompute: {
      // require map type of current graph
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
            func_edge_mutate_ = execute_resp.func_edge_mutate;
            break;
          default:
            LOGF_ERROR("Map type is not supported!");
            break;
        }
        // read first block.
        TryReadNextGraph();
      } else {
        if (group_mode_) {
          for (int i = 0; i < group_num_; i++) {
            auto gid = group_graphs_.at(i);
            graph_state_.SetDeserializedToComputed(gid);
            ExecuteMessage execute_message;
            execute_message.graph_id = gid;
            execute_message.execute_type = ExecuteType::kSerialize;
            execute_message.graph = graph_state_.GetSubgraph(gid);
            message_hub_.get_executor_queue()->Push(execute_message);
          }
          // release the group_graph handler
          group_serializable_graph_.reset();
        } else {
          graph_state_.SetDeserializedToComputed(execute_resp.graph_id);
          ExecuteMessage execute_message(execute_resp);
          execute_message.graph_id = execute_resp.graph_id;
          execute_message.execute_type = ExecuteType::kSerialize;
          message_hub_.get_executor_queue()->Push(execute_message);
        }
      }
      break;
    }
    case ExecuteType::kSerialize: {
      graph_state_.SetComputedToSerialized(execute_resp.graph_id);
      graph_state_.ReleaseSubgraph(execute_resp.graph_id);
      // Check if current round finish.
      if (group_mode_) {
        group_serialized_num_++;
        have_read_graphs_--;
        if (group_serialized_num_ != group_num_) {
          WriteMessage write_message;
          write_message.graph_id = execute_resp.graph_id;
          write_message.serialized = execute_resp.serialized;
          message_hub_.get_writer_queue()->Push(write_message);
          break;
        } else {
          auto left = GetLeftPendingGraphNums();
          group_num_ = left >= need_read_graphs_ ? need_read_graphs_ : left;
          group_serialized_num_ = 0;
          group_deserialized_num_ = 0;
          //          have_read_graphs_ = 0;
          group_graphs_.clear();
        }
      }

      if (IsCurrentRoundFinish()) {
        // TODO: sync after all sub_graphs are written back.
        // This sync maybe replaced by borderVertex check.
        graph_state_.ResetCurrentRoundPending();
        update_store_->Sync();
        LOGF_INFO(" Current MapType: {}, Step: {}", current_Map_type_, step_);
        step_++;
        current_Map_type_ = MapType::kDefault;
        func_vertex_ = nullptr;
        func_edge_ = nullptr;
        func_edge_mutate_ = nullptr;

        // inform this map exe is over

        if (group_mode_) {
          auto left = GetLeftPendingGraphNums();
          group_num_ = left >= need_read_graphs_ ? need_read_graphs_ : left;
          group_serialized_num_ = 0;
          group_deserialized_num_ = 0;
          group_graphs_.clear();
        }
        LOGF_INFO(" current read input size: {}", loader_->SizeOfReadNow());

        if (short_cut_) {
          // Keep the last graph in memory and execute first in next round.
          graph_state_.SetComputedSerializedToReadSerialized(
              execute_resp.graph_id);
        } else {
          is_executor_running_ = false;
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
          // first write back to disk
          WriteMessage write_message;
          write_message.graph_id = execute_resp.graph_id;
          write_message.serialized = execute_resp.serialized;
          message_hub_.get_writer_queue()->Push(write_message);
          // second execute next ready subgraph if exist
          auto next_execute_gid = GetNextExecuteGraph();
          if (next_execute_gid != INVALID_GRAPH_ID) {
            if (group_mode_) {
              if (have_read_graphs_ >= group_num_) {
                GetNextExecuteGroupGraphs();
                for (int i = 0; i < group_num_; i++) {
                  auto gid = group_graphs_.at(i);
                  ExecuteMessage execute_message;
                  execute_message.graph_id = gid;
                  execute_message.serialized =
                      graph_state_.GetSubgraphSerialized(gid);
                  CreateSerializableGraph(gid);
                  execute_message.graph = graph_state_.GetSubgraph(gid);
                  execute_message.execute_type = ExecuteType::kDeserialize;
                  LOGF_INFO(
                      "GROUP MODE: deserialize and execute the read graph {}",
                      gid);
                  is_executor_running_ = true;
                  message_hub_.get_executor_queue()->Push(execute_message);
                }
              } else {
                is_executor_running_ = false;
              }
            } else {
              ExecuteMessage execute_message;
              execute_message.graph_id = next_execute_gid;
              execute_message.serialized =
                  graph_state_.GetSubgraphSerialized(next_execute_gid);
              CreateSerializableGraph(next_execute_gid);
              execute_message.graph =
                  graph_state_.GetSubgraph(next_execute_gid);
              execute_message.execute_type = ExecuteType::kDeserialize;
              execute_message.map_type = current_Map_type_;
              is_executor_running_ = true;
              message_hub_.get_executor_queue()->Push(execute_message);
            }
          } else {
            is_executor_running_ = false;
          }
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

bool PramScheduler::WriteMessageResponseAndCheckTerminate(
    const WriteMessage& write_resp) {
  // Update subgraph state.
  graph_state_.SetSerializedToOnDisk(write_resp.graph_id);
  graph_state_.ReleaseSubgraphSerialized(write_resp.graph_id);
  auto write_size = graph_metadata_info_.GetSubgraphSize(write_resp.graph_id);
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
  }
  //  have_read_graphs_--;
  // update subgraph size in memory

  graph_metadata_info_.UpdateBlockSize(write_resp.graph_id);
  // Read next subgraph if permitted
  if (current_Map_type_ == MapType::kDefault) {
    std::lock_guard<std::mutex> lock(pram_mtx_);
    pram_ready_ = true;
    pram_cv_.notify_all();
  } else {
    TryReadNextGraph();
  }
  return true;
}

// private methods:

// try to read a graph from disk into memory if memory_limit is permitted
bool PramScheduler::TryReadNextGraph(bool sync) {
  bool read_flag = false;
  // group_mode use the same logic
  if (use_limits_) {
    if (limits_ > 0) read_flag = true;
  } else {
    if (memory_left_size_ > 0) read_flag = true;
  }
  //  if (memory_left_size_ > 0) {
  if (read_flag) {
    auto next_graph_id = GetNextReadGraphInCurrentRound();
    ReadMessage read_message;
    if (next_graph_id != INVALID_GRAPH_ID) {
      if (use_limits_) {
        limits_--;
        //        LOGF_INFO("Read on graph {}. now limits is {}", next_graph_id,
        //        limits_);
      } else {
        auto read_size = graph_metadata_info_.GetSubgraphSize(next_graph_id);
        if (memory_left_size_ < read_size) {
          // Memory is not enough, return.
          return false;
        }
        LOGF_INFO(
            "To read subgraph {}. *** Memory size now: {}, after read: {} "
            "***",
            next_graph_id, memory_left_size_, memory_left_size_ - read_size);
        memory_left_size_ -= read_size;
      }
      to_read_graphs_--;
      read_message.graph_id = next_graph_id;
      read_message.num_vertices =
          graph_metadata_info_.GetBlockNumVertices(next_graph_id);
      read_message.response_serialized = CreateSerialized(next_graph_id);
      graph_state_.SetOnDiskToReading(next_graph_id);
      message_hub_.get_reader_queue()->Push(read_message);
    }
  }
  return true;
}

void PramScheduler::CreateSerializableGraph(common::GraphID graph_id) {
  if (common::Configurations::Get()->vertex_data_type ==
      common::VertexDataType::kVertexDataTypeUInt32) {
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
}

core::data_structures::Serialized* PramScheduler::CreateSerialized(
    common::GraphID graph_id) {
  return graph_state_.NewSerializedBlockGraph(graph_id);
}

// release all graphs in memory. not write back to disk. just release memory.
void PramScheduler::ReleaseAllGraph() {
  for (int i = 0; i < graph_state_.num_blocks_; i++) {
    if (graph_state_.subgraph_storage_state_.at(i) == GraphState::Serialized)
      graph_state_.ReleaseSubgraphSerialized(i);
  }
}

void PramScheduler::SetAppRuntimeGraph(common::GraphID gid) {
  if (group_mode_) {
  } else {
    if (common::Configurations::Get()->vertex_data_type ==
        common::VertexDataType::kVertexDataTypeUInt32) {
    } else {
    }
  }
}

void PramScheduler::SetGlobalVertexData(common::GraphID bid) {
  auto block = (BlockCSRGraphUInt32*)graph_state_.GetSubgraph(bid);
  block->SetGlobalVertexData(update_store_);
}

common::GraphID PramScheduler::GetNextReadGraphInCurrentRound() const {
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) == GraphState::OnDisk) {
      return gid;
    }
  }
  return INVALID_GRAPH_ID;
}

common::GraphID PramScheduler::GetNextExecuteGraph() const {
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) ==
            GraphState::StorageStateType::Serialized) {
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

common::GraphID PramScheduler::GetNextReadGraphInNextRound() const {
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

bool PramScheduler::IsSystemStop() const { return true; }

void PramScheduler::RunMapExecute(ExecuteMessage execute_msg) {
  message_hub_.get_response_queue()->Push(scheduler::Message(execute_msg));
}

void PramScheduler::SetExecuteMessageMapFunction(ExecuteMessage* message) {
  switch (current_Map_type_) {
    case MapType::kMapVertex:
      message->func_vertex = func_vertex_;
      break;
    case MapType::kMapEdge:
      message->func_edge = func_edge_;
      break;
    case MapType::kMapEdgeAndMutate:
      message->func_edge_mutate = func_edge_mutate_;
      break;
    default:
      LOGF_ERROR("Map type is not supported!");
      break;
  }
}

}  // namespace sics::graph::nvme::scheduler
