#include "scheduler2.h"

namespace sics::graph::core::scheduler {

void Scheduler2::Start() {
  thread_ = std::make_unique<std::thread>([this]() {
    // auto update_store_size = update_store_->GetMemorySize();
    //    memory_left_size_ -= update_store_size;
    LOGF_INFO("global memory size: {} MB", buffer_->GetBufferSize());
    LOGF_INFO(" ============ Current Round: {} ============ ", current_round_);
    bool running = true;
    // init round 0 loaded graph

    //      ReadMessage first_read_message;
    //      first_read_message.graph_id = GetNextReadGraphInCurrentRound();
    //      first_read_message.round = 0;

    //      if (use_limits_) {
    //        limits_--;
    //        if (limits_ < 0) {
    //          LOG_FATAL("no limits left, error!");
    //        }
    //      } else {
    //        graph_state_.subgraph_limits_--;
    //        auto read_size =
    //            graph_metadata_info_.GetSubgraphSize(first_read_message.graph_id);
    //        if (memory_left_size_ < read_size) {
    //          LOG_FATAL("read size is too large, memory is not enough!");
    //        }
    //        LOGF_INFO(
    //            "read graph {} size: {}. *** Memory size now: {}, after: {}
    //            ***", first_read_message.graph_id, read_size,
    //            memory_left_size_, memory_left_size_ - read_size);
    //        memory_left_size_ -= read_size;
    //      }
    //      graph_state_.SetOnDiskToReading(first_read_message.graph_id);
    //      buffer_.SetBlockReading(first_read_message.graph_id);
    //      message_hub_.get_reader_queue()->Push(first_read_message);
    auto id = GetNextExecuteGraph();
    ExecuteMessage exe_messgae;
    exe_messgae.graph_id = id;
    app_->SetRound(current_round_);
    exe_messgae.app = app_;
    exe_messgae.execute_type = GetExecuteType(id);
    message_hub_.get_executor_queue()->Push(exe_messgae);

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

bool Scheduler2::ReadMessageResponseAndExecute(const ReadMessage& read_resp) {
  return true;
}

bool Scheduler2::ExecuteMessageResponseAndWrite(
    const ExecuteMessage& execute_resp) {
  // Execute finish, decide next executor work.
  //  is_executor_running_ = false;
  switch (execute_resp.execute_type) {
    case ExecuteType::kPEval:
    case ExecuteType::kIncEval: {
      graph_state_.SetGraphState(execute_resp.graph_id,
                                 GraphState::StorageStateType::Computed);
      graph_state_.SetGraphCurrentRoundFinish(execute_resp.graph_id);
      graph_state_.AddGraphRound(execute_resp.graph_id);
      if (IsCurrentRoundFinish()) {
        if (IsSystemStop()) {
          // Release All edge blocks.
          ReleaseAllSubEdgeBlocks();
          current_round_++;
          LOGF_INFO(" ============ Current Round: {} Finish ============ ",
                    current_round_);
          return false;
        } else {
          // TODO: sync after all sub_graphs are written back.
          // This sync maybe replaced by borderVertex check.
          graph_state_.SyncCurrentRoundPending();
          current_round_++;
          app_->SetInActive();
              LOGF_INFO(" ============ Current Round: {} Finish ============ ",
                    current_round_);

          if (short_cut_) {
            //            // Keep the last graph in memory and execute first in
            //            next round.
            //            graph_state_.SetComputedSerializedToReadSerialized(
            //                execute_resp.graph_id);
            //            ExecuteMessage execute_message;
            //            execute_message.graph_id = execute_resp.graph_id;
            //            execute_message.serialized = execute_resp.serialized;
            //            execute_message.execute_type =
            //            ExecuteType::kDeserialize; LOGF_INFO(
            //                "The last graph {} is in memory, deserialize it
            //                and execute.", execute_resp.graph_id);
            //            message_hub_.get_executor_queue()->Push(execute_message);
          } else {
            // First, release last subgraph edge blocks.
            ReleaseAllGraph(execute_resp.graph_id);
            auto next_id = GetNextExecuteGraph();
            ExecuteMessage exe_next;
            exe_next.graph_id = next_id;
            app_->SetRound(current_round_);
            exe_next.app = app_;
            exe_next.execute_type = GetExecuteType(next_id);
            message_hub_.get_executor_queue()->Push(exe_next);
          }
        }
      } else {
        // Release first
        ReleaseAllGraph(execute_resp.graph_id);
        auto next_execute_gid = GetNextExecuteGraph();
        if (next_execute_gid == INVALID_GRAPH_ID)
          LOG_FATAL("Error at Get gid for executing!");
        ExecuteMessage execute_message;
        execute_message.graph_id = next_execute_gid;
        app_->SetRound(current_round_);
        execute_message.execute_type = GetExecuteType(next_execute_gid);
        execute_message.app = app_;
        message_hub_.get_executor_queue()->Push(execute_message);
      }
      break;
    }
    default:
      LOG_WARN("Executor response show it doing nothing!");
      break;
  }
  return true;
}

bool Scheduler2::WriteMessageResponseAndCheckTerminate(
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
bool Scheduler2::TryReadNextGraph(bool sync) {
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
      graph_state_.SetOnDiskToReading(next_graph_id);
      message_hub_.get_reader_queue()->Push(read_message);
    } else {
      // TODO: fix this if branch
      // check next round graph which can be read, if not just skip
      //      if (sync) {
      //        current_round_++;
      //      }
      //      auto next_gid_next_round = GetNextReadGraphInNextRound();
      //      if (next_gid_next_round != INVALID_GRAPH_ID) {
      //        if (threefour_mode_) {
      //          LOG_INFO("load on more graph");
      //        } else {
      //          if (use_limits_) {
      //            limits_--;
      //            LOGF_INFO("Read on graph. now limits is {}", limits_);
      //          } else {
      //            auto read_size =
      //                graph_metadata_info_.GetSubgraphSize(next_gid_next_round);
      //            if (memory_left_size_ < read_size) {
      //              return false;
      //            }
      //            LOGF_INFO(
      //                "To read subgraph {}. *** Memory size now: {}, after
      //                read: "
      //                "{} "
      //                "***",
      //                next_gid_next_round, memory_left_size_,
      //                memory_left_size_ - read_size);
      //            memory_left_size_ -= read_size;
      //          }
      //        }
      //        read_message.graph_id = next_gid_next_round;
      //        read_message.num_vertices =
      //            graph_metadata_info_.GetSubgraphNumVertices(read_message.graph_id);
      //        read_message.round =
      //            graph_state_.GetSubgraphRound(read_message.graph_id);
      //        read_message.response_serialized =
      //            CreateSerialized(next_gid_next_round);
      //        message_hub_.get_reader_queue()->Push(read_message);
      //      } else {
      //        // no graph can be read, terminate system
      //        return false;
      //      }
    }
    graph_state_.subgraph_limits_--;
  }
  return true;
}

void Scheduler2::InitGroupSerializableGraph() {}

void Scheduler2::SetAppRuntimeGraph(common::GraphID gid) {}

// TODO: Add logic to decide which graph is read first.
common::GraphID Scheduler2::GetNextReadGraphInCurrentRound() const {
  for (GraphID gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) == GraphState::OnDisk) {
      return gid;
    }
  }
  return INVALID_GRAPH_ID;
}

// TODO: Add logic to decide which graph is executed first.
common::GraphID Scheduler2::GetNextExecuteGraph() const {
  for (GraphID gid = 0; gid < metadata_->num_blocks; gid++) {
    if (graph_state_.current_round_pending_.at(gid) &&
        graph_state_.subgraph_round_.at(gid) == current_round_) {
      return gid;
    }
  }
  return INVALID_GRAPH_ID;
}

void Scheduler2::GetNextExecuteGroupGraphs() {
  size_t count = 0;
  for (GraphID gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
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

common::GraphID Scheduler2::GetNextReadGraphInNextRound() const {
  for (GraphID gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.next_round_pending_.at(gid) &&
        graph_state_.subgraph_storage_state_.at(gid) ==
            GraphState::StorageStateType::OnDisk) {
      return gid;
    }
  }
  return INVALID_GRAPH_ID;
}

size_t Scheduler2::GetLeftPendingGraphNums() const {
  size_t res = 0;
  for (GraphID gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
    if (graph_state_.current_round_pending_.at(gid)) {
      res++;
    }
  }
  return res;
}

bool Scheduler2::IsCurrentRoundFinish() const {
  for (GraphID i = 0; i < graph_metadata_info_.get_num_subgraphs(); i++) {
    if (graph_state_.current_round_pending_.at(i)) {
      return false;
    }
  }
  return true;
}

bool Scheduler2::IsSystemStop() const { return app_->IsActive() == 0; }

}  // namespace sics::graph::core::scheduler