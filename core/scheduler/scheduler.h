#ifndef GRAPH_SYSTEMS_SCHEDULER_H
#define GRAPH_SYSTEMS_SCHEDULER_H

#include "data_structures/graph_metadata.h"
#include "scheduler/message_hub.h"
#include "scheduler/subgraph_state.h"

namespace sics::graph::core::scheduler {

// template <typename VertexData, typename EdgeData>
class Scheduler {
 public:
  Scheduler() = default;
  virtual ~Scheduler() = default;

  int GetCurrentRound() const { return current_round_; }

  // read graph metadata from meta.yaml file
  // meta file path should be passed by gflags
  void ReadGraphMetadata(const std::string& graph_metadata_path) {
    YAML::Node graph_metadata_node;
    try {
      graph_metadata_node = YAML::LoadFile(graph_metadata_path);
      graph_metadata_info_ = graph_metadata_node["GraphMetadata"]
                                 .as<data_structures::GraphMetadata>();
    } catch (YAML::BadFile& e) {
      LOG_FATAL("meta.yaml file read failed! ", e.msg);
    }
  }

  // get message hub ptr for other component
  MessageHub* get_message_hub() { return &message_hub_; }

  // global message store

  //  VertexData ReadGlobalMessage(common::VertexID vid) const {
  //    return global_message_read_[vid];
  //  }
  //
  //  bool WriteGlobalMessage(common::VertexID vid, VertexData data) {
  //    global_message_write_[vid] = data;
  //    return true;
  //  }
  //
  //  bool SyncGlobalMessage() {
  //    memcpy(global_message_read_, global_message_write_,
  //           global_message_size_ * sizeof(VertexData));
  //    return true;
  //  }

  // schedule subgraph execute and its IO(read and write)
  void Start() {
    thread_ = std::make_unique<std::thread>([this]() {
      bool running = true;
      // init round 0 loaded graph
      graph_state_.Init();
      ReadMessage first_read_message;
      first_read_message.graph_id = GetNextReadGraphInCurrentRound();
      message_hub_.get_reader_queue()->Push(first_read_message);

      while (running) {
        Message resp = message_hub_.GetResponse();

        if (resp.is_terminated()) {
          LOG_INFO("*** Scheduler is signaled termination ***");
          break;
        }
        switch (resp.get_type()) {
          case Message::Type::kRead: {
            ReadMessageResponseAndExecute(resp);
            break;
          }
          case Message::Type::kExecute: {
            ExecuteMessageResponseAndWrite(resp);
            break;
          }
          case Message::Type::kWrite: {
            if (!WriteMessageResponseAndCheckTerminate(resp)) {
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

 protected:
  virtual bool ReadMessageResponseAndExecute(const Message& resp) {
    // read finish, to execute the loaded graph
    ReadMessage read_response;
    resp.Get(&read_response);
    graph_state_.SetGraphState(read_response.graph_id,
                               GraphState::StorageStateType::Serialized);

    // read graph need deserialize first
    // check current round subgraph and send it to executer to Deserialization.
    if (graph_state_.GetSubgraphRound(read_response.graph_id) ==
        current_round_) {
      ExecuteMessage execute_message;
      execute_message.graph_id = read_response.graph_id;
      execute_message.serialized = read_response.response_serialized;
      execute_message.execute_type = ExecuteType::kDeserialize;
      message_hub_.get_executor_queue()->Push(execute_message);
    } else {
      //      graph_state_.SetSubgraphSerialized(read_response.graph_id,
      //                                         read_response.response_serialized);
      graph_state_.SetSubgraphSerialized(
          read_response.graph_id, std::unique_ptr<data_structures::Serialized>(
                                      read_response.response_serialized));
    }

    TryReadNextGraph();
    return true;
  }

  virtual bool ExecuteMessageResponseAndWrite(const Message& resp) {
    // execute finish, to write back the graph
    ExecuteMessage execute_response;
    resp.Get(&execute_response);

    switch (execute_response.execute_type) {
      case ExecuteType::kDeserialize: {
        ExecuteMessage execute_message;
        execute_message.graph_id = execute_response.graph_id;
        execute_message.graph = execute_response.response_serializable;
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
            execute_response.execute_type = ExecuteType::kSerialize;
            message_hub_.get_executor_queue()->Push(execute_response);
          }
        }
        // check border vertex and dependency matrix, mark active subgraph in
        // next round
        // TODO: check border vertex and dependency matrix
        break;
      }
      case ExecuteType::kSerialize: {
        WriteMessage write_message;
        write_message.graph_id = execute_response.graph_id;
        write_message.serializable = execute_response.response_serializable;
        message_hub_.get_writer_queue()->Push(write_message);
        break;
      }
      default:
        LOG_WARN("Executer response show it doing nothing!");
        break;
    }

    TryReadNextGraph();
    return true;
  }

  bool WriteMessageResponseAndCheckTerminate(const Message& resp) {
    // TODO: check memory size to read new subgraph
    WriteMessage write_response;
    resp.Get(&write_response);
    graph_state_.SetGraphState(write_response.graph_id,
                               GraphState::StorageStateType::OnDisk);
    if (true) {
      // check if read next round graph
      auto current_round_next_graph_id = GetNextReadGraphInCurrentRound();
      if (current_round_next_graph_id == INVALID_GRAPH_ID) {
        current_round_++;
        graph_state_.SyncRoundState();
        auto next_round_first_graph_id = GetNextReadGraphInNextRound();
        if (next_round_first_graph_id == INVALID_GRAPH_ID) {
          // no graph should be loaded, terminate system
          return false;
        } else {
          ReadMessage read_message;
          read_message.graph_id = next_round_first_graph_id;
          message_hub_.get_reader_queue()->Push(read_message);
        }
      }
    }
    return true;
  }

 private:
  // TODO: check memory size to decide if read next graph.
  // try to read next graph will be executed.
  // in current round or next round
  bool TryReadNextGraph() {
    if (true) {
      auto next_graph_id = GetNextReadGraphInCurrentRound();
      ReadMessage read_message;
      if (next_graph_id != INVALID_GRAPH_ID) {
        read_message.graph_id = next_graph_id;
        message_hub_.get_reader_queue()->Push(read_message);
      } else {
        // check next round graph which can be read, if not just skip
        auto next_gid_next_round = GetNextReadGraphInNextRound();
        if (next_gid_next_round != INVALID_GRAPH_ID) {
          read_message.graph_id = next_gid_next_round;
          message_hub_.get_reader_queue()->Push(read_message);
        }
      }
    }
    return true;
  }

  // move to scheduler
  common::GraphID GetNextReadGraphInCurrentRound() const {
    for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
      if (graph_state_.current_round_pending_.at(gid) &&
          graph_state_.subgraph_storage_state_.at(gid) ==
              GraphState::StorageStateType::OnDisk) {
        return gid;
      }
    }
    return INVALID_GRAPH_ID;
  }

  // get next execute graph in current round
  common::GraphID GetNextExecuteGraph() const {
    for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
      if (graph_state_.current_round_pending_.at(gid) &&
          graph_state_.subgraph_storage_state_.at(gid) ==
              GraphState::StorageStateType::Deserialized) {
        return gid;
      }
    }
    return INVALID_GRAPH_ID;
  }

  common::GraphID GetNextReadGraphInNextRound() {
    for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
      if (graph_state_.next_round_pending_.at(gid) &&
          graph_state_.subgraph_storage_state_.at(gid) ==
              GraphState::StorageStateType::OnDisk) {
        return gid;
      }
    }
    return INVALID_GRAPH_ID;
  }

  bool IsCurrentRoundFinish() {
    return GetNextReadGraphInCurrentRound() == INVALID_GRAPH_ID;
  }

  bool IsSystemStop() {
    // if current and next round both have no graph to read, system stop
    return (GetNextReadGraphInCurrentRound() == INVALID_GRAPH_ID) &&
           (GetNextReadGraphInNextRound() == INVALID_GRAPH_ID);
  }

 private:
  // graph metadata: graph info, dependency matrix, subgraph metadata, etc.
  data_structures::GraphMetadata graph_metadata_info_;
  GraphState graph_state_;

  int current_round_ = 0;

  // message hub
  MessageHub message_hub_;
  // global message store
  //  VertexData* global_message_read_;
  //  VertexData* global_message_write_;
  //  common::VertexCount global_message_size_;

  std::unique_ptr<std::thread> thread_;
};

}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_SCHEDULER_H
