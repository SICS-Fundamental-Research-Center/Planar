#include "scheduler.h"

namespace sics::graph::core::scheduler {

void Scheduler::Start() {
  thread_ = std::make_unique<std::thread>([this]() {
    auto update_store_size = update_store_->GetMemorySize();
    //    memory_left_size_ -= update_store_size;
    LOGF_INFO("global memory size: {} MB", memory_left_size_);
    LOGF_INFO(" ============ Current Round: {} ============ ", current_round_);
    bool running = true;
    // init round 0 loaded graph
    if (threefour_mode_) {
      need_read_graphs_ = 3;
      to_read_graphs_ = 3;
      have_read_graphs_ = 0;
      // load three subgraph
      to_read_graphs_--;
      ReadMessage first_read_message;
      first_read_message.graph_id = GetNextReadGraphInCurrentRound();
      first_read_message.num_vertices =
          graph_metadata_info_.GetSubgraphNumVertices(
              first_read_message.graph_id);
      first_read_message.response_serialized =
          CreateSerialized(first_read_message.graph_id);
      first_read_message.round = 0;
      graph_state_.SetOnDiskToReading(first_read_message.graph_id);
      message_hub_.get_reader_queue()->Push(first_read_message);
    } else {
      if (group_mode_) {
        group_serialized_num_ = 0;
        group_deserialized_num_ = 0;
        have_read_graphs_ = 0;
        need_read_graphs_ = group_num_;
        LOGF_INFO("Group Mode: group_num {}", group_num_);
      }
      ReadMessage first_read_message;
      first_read_message.graph_id = GetNextReadGraphInCurrentRound();
      first_read_message.num_vertices =
          graph_metadata_info_.GetSubgraphNumVertices(
              first_read_message.graph_id);
      first_read_message.response_serialized =
          CreateSerialized(first_read_message.graph_id);
      first_read_message.round = 0;

      if (use_limits_) {
        limits_--;
        if (limits_ < 0) {
          LOG_FATAL("no limits left, error!");
        }
      } else {
        graph_state_.subgraph_limits_--;
        auto read_size =
            graph_metadata_info_.GetSubgraphSize(first_read_message.graph_id);
        if (memory_left_size_ < read_size) {
          LOG_FATAL("read size is too large, memory is not enough!");
        }
        LOGF_INFO(
            "read graph {} size: {}. *** Memory size now: {}, after: {} ***",
            first_read_message.graph_id, read_size, memory_left_size_,
            memory_left_size_ - read_size);
        memory_left_size_ -= read_size;
      }
      graph_state_.SetOnDiskToReading(first_read_message.graph_id);
      message_hub_.get_reader_queue()->Push(first_read_message);
    }
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
    LOGF_INFO(" ============ Current Round: {} ============ ", current_round_);
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
    if (threefour_mode_) {
      have_read_graphs_++;
      LOGF_INFO("already read graph number: {}", have_read_graphs_);
      if (have_read_graphs_ == need_read_graphs_) {
        // When executor is working.
        if (!is_executor_running_) {
          ExecuteMessage execute_message;
          execute_message.graph_id = read_resp.graph_id;
          execute_message.serialized = read_resp.response_serialized;
          CreateSerializableGraph(read_resp.graph_id);
          execute_message.graph = graph_state_.GetSubgraph(read_resp.graph_id);
          execute_message.execute_type = ExecuteType::kDeserialize;
          is_executor_running_ = true;
          LOGF_INFO("read finished, deserialize and execute the read graph {}",
                    read_resp.graph_id);
          message_hub_.get_executor_queue()->Push(execute_message);
        } else {
          // When executor is running, do nothing, wait for executor finish.
        }
      }
    } else {
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
              execute_message.serialized =
                  graph_state_.GetSubgraphSerialized(gid);
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
          LOGF_INFO("read finished, deserialize and execute the read graph {}",
                    read_resp.graph_id);
          message_hub_.get_executor_queue()->Push(execute_message);
        } else {
          // When executor is running, do nothing, wait for executor finish.
        }
      }
    }
  } else {
    // TODO: do something when load next round subgraph
  }
  TryReadNextGraph();
  return true;
}

bool Scheduler::ExecuteMessageResponseAndWrite(
    const ExecuteMessage& execute_resp) {
  // Execute finish, decide next executor work.
  //  is_executor_running_ = false;
  switch (execute_resp.execute_type) {
    case ExecuteType::kDeserialize: {
      graph_state_.SetSerializedToDeserialized(execute_resp.graph_id);
      if (group_mode_) {
        group_deserialized_num_++;
        if (group_deserialized_num_ == group_num_) {
          CreateGroupSerializableGraph();

          ExecuteMessage execute_message;
          execute_message.graph_id = group_graphs_.at(0);
          execute_message.graph = group_serializable_graph_.get();
          if (current_round_ == 0) {
            execute_message.execute_type = ExecuteType::kPEval;
          } else {
            execute_message.execute_type = ExecuteType::kIncEval;
          }
          SetAppRuntimeGraph(group_graphs_.at(0));
          SetAppRound(current_round_);
          execute_message.app = app_;
          message_hub_.get_executor_queue()->Push(execute_message);
        }
      } else {
        ExecuteMessage execute_message;
        execute_message.graph_id = execute_resp.graph_id;
        execute_message.graph = execute_resp.response_serializable;
        if (current_round_ == 0) {
          execute_message.execute_type = ExecuteType::kPEval;
        } else {
          execute_message.execute_type = ExecuteType::kIncEval;
        }
        SetAppRuntimeGraph(execute_message.graph_id);
        SetAppRound(current_round_);
        execute_message.app = app_;
        message_hub_.get_executor_queue()->Push(execute_message);
      }
      break;
    }
    case ExecuteType::kPEval:
    case ExecuteType::kIncEval: {
      // TODO: decide a subgraph if it stays in memory
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
        if (IsSystemStop()) {
          WriteMessage write_message;
          write_message.graph_id = execute_resp.graph_id;
          write_message.serialized = execute_resp.serialized;
          message_hub_.get_writer_queue()->Push(write_message);

          // TODO: Just release memory of serialized graph, not write back.
          //          ReleaseAllGraph();
          return false;
        } else {
          // TODO: sync after all sub_graphs are written back.
          // This sync maybe replaced by borderVertex check.
          auto active = update_store_->GetActiveCount();
          graph_state_.SyncCurrentRoundPending();
          update_store_->Sync();
          current_round_++;
          if (group_mode_) {
            auto left = GetLeftPendingGraphNums();
            group_num_ = left >= need_read_graphs_ ? need_read_graphs_ : left;
            group_serialized_num_ = 0;
            group_deserialized_num_ = 0;
            group_graphs_.clear();
          }
          LOGF_INFO(
              " ============ Current Round: {}, Active vertex left: {} "
              "============ ",
              current_round_, active);
          LOGF_INFO(" current read input size: {}", loader_->SizeOfReadNow());

          if (short_cut_) {
            // Keep the last graph in memory and execute first in next round.
            graph_state_.SetComputedSerializedToReadSerialized(
                execute_resp.graph_id);
            ExecuteMessage execute_message;
            execute_message.graph_id = execute_resp.graph_id;
            execute_message.serialized = execute_resp.serialized;
            CreateSerializableGraph(execute_resp.graph_id);
            execute_message.graph =
                graph_state_.GetSubgraph(execute_resp.graph_id);
            execute_message.execute_type = ExecuteType::kDeserialize;
            is_executor_running_ = true;
            LOGF_INFO(
                "The last graph {} is in memory, deserialize it and execute.",
                execute_resp.graph_id);
            message_hub_.get_executor_queue()->Push(execute_message);
          } else {
            is_executor_running_ = false;
            WriteMessage write_message;
            write_message.graph_id = execute_resp.graph_id;
            write_message.serialized = execute_resp.serialized;
            message_hub_.get_writer_queue()->Push(write_message);
          }
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

bool Scheduler::WriteMessageResponseAndCheckTerminate(
    const WriteMessage& write_resp) {
  // Update subgraph state.
  graph_state_.subgraph_limits_++;
  graph_state_.SetSerializedToOnDisk(write_resp.graph_id);
  graph_state_.ReleaseSubgraphSerialized(write_resp.graph_id);
  auto write_size = graph_metadata_info_.GetSubgraphSize(write_resp.graph_id);
  LOGF_INFO(
      "Release subgraph: {}, size: {}. *** Memory size now: {}, after: {} "
      "***",
      write_resp.graph_id, write_size, memory_left_size_,
      memory_left_size_ + write_size);
  if (threefour_mode_) {
    LOG_INFO("Write back one graph");
  } else {
    if (use_limits_) {
      limits_++;
      LOGF_INFO("Write back one graph. now limits is {}", limits_);
    } else {
      memory_left_size_ += write_size;
    }
  }
  //  have_read_graphs_--;
  // update subgraph size in memory
  graph_metadata_info_.UpdateSubgraphSize(write_resp.graph_id);
  // Read next subgraph if permitted
  TryReadNextGraph();
  return true;
}

// private methods:

// try to read a graph from disk into memory if memory_limit is permitted
bool Scheduler::TryReadNextGraph(bool sync) {
  //  if (graph_state_.subgraph_limits_ > 0) {
  bool read_flag = false;
  if (threefour_mode_) {
    if (to_read_graphs_ == 0) {
      if (!is_executor_running_ && (have_read_graphs_ == need_read_graphs_)) {
        auto left = GetLeftPendingGraphNums();
        to_read_graphs_ = left >= 3 ? 3 : left;
        need_read_graphs_ = to_read_graphs_;
        have_read_graphs_ = 0;
        LOGF_INFO("Next need read numbers: {}", to_read_graphs_);
        read_flag = true;
      } else {
        return false;
      }
    } else {
      read_flag = true;
    }
  } else {
    // group_mode use the same logic
    if (use_limits_) {
      if (limits_ > 0) read_flag = true;
    } else {
      if (memory_left_size_ > 0) read_flag = true;
    }
  }
  //  if (memory_left_size_ > 0) {
  if (read_flag) {
    auto next_graph_id = GetNextReadGraphInCurrentRound();
    ReadMessage read_message;
    if (next_graph_id != INVALID_GRAPH_ID) {
      if (threefour_mode_) {
        LOG_INFO("load on more graph");
      } else {
        if (use_limits_) {
          limits_--;
          LOGF_INFO("Read on graph. now limits is {}", limits_);
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
      }
      to_read_graphs_--;
      read_message.graph_id = next_graph_id;
      read_message.num_vertices =
          graph_metadata_info_.GetSubgraphNumVertices(next_graph_id);
      read_message.round = graph_state_.GetSubgraphRound(next_graph_id);
      read_message.response_serialized = CreateSerialized(next_graph_id);
      graph_state_.SetOnDiskToReading(next_graph_id);
      message_hub_.get_reader_queue()->Push(read_message);
    } else {
      // TODO: fix this if branch
      // check next round graph which can be read, if not just skip
      if (sync) {
        current_round_++;
      }
      auto next_gid_next_round = GetNextReadGraphInNextRound();
      if (next_gid_next_round != INVALID_GRAPH_ID) {
        if (threefour_mode_) {
          LOG_INFO("load on more graph");
        } else {
          if (use_limits_) {
            limits_--;
            LOGF_INFO("Read on graph. now limits is {}", limits_);
          } else {
            auto read_size =
                graph_metadata_info_.GetSubgraphSize(next_gid_next_round);
            if (memory_left_size_ < read_size) {
              return false;
            }
            LOGF_INFO(
                "To read subgraph {}. *** Memory size now: {}, after read: "
                "{} "
                "***",
                next_gid_next_round, memory_left_size_,
                memory_left_size_ - read_size);
            memory_left_size_ -= read_size;
          }
        }
        read_message.graph_id = next_gid_next_round;
        read_message.num_vertices =
            graph_metadata_info_.GetSubgraphNumVertices(read_message.graph_id);
        read_message.round =
            graph_state_.GetSubgraphRound(read_message.graph_id);
        read_message.response_serialized =
            CreateSerialized(next_gid_next_round);
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

void Scheduler::CreateSerializableGraph(common::GraphID graph_id) {
  if (common::Configurations::Get()->vertex_data_type ==
      common::VertexDataType::kVertexDataTypeUInt32) {
    std::unique_ptr<data_structures::Serializable> serializable_graph =
        std::make_unique<MutableCSRGraphUInt32>(
            graph_metadata_info_.GetSubgraphMetadataPtr(graph_id),
            graph_metadata_info_.get_num_vertices());
    graph_state_.SetSubGraph(graph_id, std::move(serializable_graph));
  } else {
    std::unique_ptr<data_structures::Serializable> serializable_graph =
        std::make_unique<MutableCSRGraphUInt16>(
            graph_metadata_info_.GetSubgraphMetadataPtr(graph_id),
            graph_metadata_info_.get_num_vertices());
    graph_state_.SetSubGraph(graph_id, std::move(serializable_graph));
  }
}

data_structures::Serialized* Scheduler::CreateSerialized(
    common::GraphID graph_id) {
  return graph_state_.NewSerializedMutableCSRGraph(graph_id);
}

void Scheduler::CreateGroupSerializableGraph() {
  //  if (common::Configurations::Get()->vertex_data_type ==
  //      common::VertexDataType::kVertexDataTypeUInt32) {
  //    group_serializable_graph_ =
  //    std::make_unique<MutableGroupCSRGraphUInt32>();
  //  } else {
  //    group_serializable_graph_ =
  //    std::make_unique<MutableGroupCSRGraphUInt16>();
  //  }
  group_serializable_graph_ = std::make_unique<MutableGroupCSRGraphUInt32>();
  InitGroupSerializableGraph();
}

void Scheduler::InitGroupSerializableGraph() {
  for (int i = 0; i < group_num_; i++) {
    auto gid = group_graphs_.at(i);
    auto group_graph =
        (MutableGroupCSRGraphUInt32*)(group_serializable_graph_.get());
    group_graph->AddSubgraph(
        (MutableCSRGraphUInt32*)graph_state_.GetSubgraph(gid));
  }
}

// release all graphs in memory. not write back to disk. just release memory.
void Scheduler::ReleaseAllGraph() {
  for (int i = 0; i < graph_state_.num_subgraphs_; i++) {
    if (graph_state_.subgraph_storage_state_.at(i) == GraphState::Serialized)
      graph_state_.ReleaseSubgraphSerialized(i);
  }
}

void Scheduler::SetAppRuntimeGraph(common::GraphID gid) {
  if (group_mode_) {
    auto app =
        dynamic_cast<apis::PlanarAppGroupBase<MutableGroupCSRGraphUInt32>*>(
            app_);
    app->SetRuntimeGraph(dynamic_cast<MutableGroupCSRGraphUInt32*>(
        group_serializable_graph_.get()));
  } else {
    if (common::Configurations::Get()->vertex_data_type ==
        common::VertexDataType::kVertexDataTypeUInt32) {
      auto app =
          dynamic_cast<apis::PlanarAppBase<MutableCSRGraphUInt32>*>(app_);
      app->SetRuntimeGraph(
          dynamic_cast<MutableCSRGraphUInt32*>(graph_state_.GetSubgraph(gid)));
    } else {
      auto app =
          dynamic_cast<apis::PlanarAppBase<MutableCSRGraphUInt16>*>(app_);
      app->SetRuntimeGraph(
          dynamic_cast<MutableCSRGraphUInt16*>(graph_state_.GetSubgraph(gid)));
    }
  }
}

void Scheduler::SetAppRound(int round) {
  if (group_mode_) {
    auto app =
        dynamic_cast<apis::PlanarAppGroupBase<MutableGroupCSRGraphUInt32>*>(
            app_);
    app->SetRound(round);
  } else {
    if (common::Configurations::Get()->vertex_data_type ==
        common::VertexDataType::kVertexDataTypeUInt32) {
      auto app =
          dynamic_cast<apis::PlanarAppBase<MutableCSRGraphUInt32>*>(app_);
      app->SetRound(round);
    } else {
      auto app =
          dynamic_cast<apis::PlanarAppBase<MutableCSRGraphUInt16>*>(app_);
      app->SetRound(round);
    }
  }
}

// TODO: Add logic to decide which graph is read first.
common::GraphID Scheduler::GetNextReadGraphInCurrentRound() const {
  //  if (test == 0) {
  //    if (graph_state_.current_round_pending_.at(4) &&
  //        graph_state_.subgraph_storage_state_.at(4) == GraphState::OnDisk) {
  //      test++;
  //      return 4;
  //    }
  //  } else if (test == 1) {
  //    if (graph_state_.current_round_pending_.at(5) &&
  //        graph_state_.subgraph_storage_state_.at(5) == GraphState::OnDisk) {
  //      test++;
  //      return 5;
  //    }
  //  }
  if (is_block_mode_) {
    for (int bid = 0; bid < graph_metadata_info_.get_num_blocks(); bid++) {
      if (graph_state_.current_round_pending_.at(bid) &&
          graph_state_.subgraph_storage_state_.at(bid) ==
              GraphState::StorageStateType::Serialized &&
          graph_state_.subgraph_round_.at(bid) == current_round_) {
        return bid;
      }
    }
    return INVALID_GRAPH_ID;
  }
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) == GraphState::OnDisk) {
      return gid;
    }
  }
  return INVALID_GRAPH_ID;
}

// TODO: Add logic to decide which graph is executed first.
common::GraphID Scheduler::GetNextExecuteGraph() const {
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) ==
            GraphState::StorageStateType::Serialized &&
        graph_state_.subgraph_round_.at(gid) == current_round_) {
      return gid;
    }
  }
  return INVALID_GRAPH_ID;
}

void Scheduler::GetNextExecuteGroupGraphs() {
  int count = 0;
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) ==
            GraphState::StorageStateType::Serialized &&
        graph_state_.subgraph_round_.at(gid) == current_round_) {
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

size_t Scheduler::GetLeftPendingGraphNums() const {
  size_t res = 0;
  for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid)) {
      res++;
    }
  }
  return res;
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