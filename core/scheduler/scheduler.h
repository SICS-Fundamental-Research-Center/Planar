#ifndef GRAPH_SYSTEMS_SCHEDULER_H
#define GRAPH_SYSTEMS_SCHEDULER_H

#include "data_structures/graph_metadata.h"
#include "message_hub.h"

namespace sics::graph::core::scheduler {

template <typename VertexData, typename EdgeData> class Scheduler {
public:
  Scheduler() = default;
  virtual ~Scheduler() = default;

  int GetCurrentRound() const { return current_round_; }

  //  does this meaningful?
  //  int GetSubgraphRound(common::GraphID subgraph_gid) const {
  //    if (graph_metadata_info_.IsSubgraphPendingCurrentRound(subgraph_gid)) {
  //      return current_round_;
  //    } else {
  //      return current_round_ + 1;
  //    }
  //  }

  // read graph metadata from meta.yaml file
  // meta file path should be passed by gflags
  void ReadGraphMetadata(const std::string &graph_metadata_path) {
    YAML::Node graph_metadata_node;
    try {
      graph_metadata_node = YAML::LoadFile(graph_metadata_path);
      graph_metadata_info_ = graph_metadata_node["GraphMetadata"]
                                 .as<data_structures::GraphMetadata>();
    } catch (YAML::BadFile &e) {
      LOG_ERROR("meta.yaml file read failed! ", e.msg);
    }
  }

  // get message hub ptr for other component
  MessageHub *get_message_hub() { return &message_hub_; }

  // global message store

  VertexData ReadGlobalMessage(common::VertexID vid) const {
    return global_message_read_[vid];
  }

  bool WriteGlobalMessage(common::VertexID vid, VertexData data) {
    global_message_write_[vid] = data;
    return true;
  }

  bool SyncGlobalMessage() {
    memcpy(global_message_read_, global_message_write_,
           global_message_size_ * sizeof(VertexData));
    return true;
  }

  // schedule read and write
  void Start() {
    thread_ = std::make_unique<std::thread>([this]() {
      bool running = true;
      // init round 0 loaded graph
      graph_metadata_info_.Init();

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

private:
  bool ReadMessageResponseAndExecute(const Message &resp) {
    // read finish, to execute the loaded graph
    ReadMessage read_response;
    resp.Get(&read_response);
    graph_metadata_info_.SetSubgraphLoaded(read_response.graph_id);

    ExecuteMessage execute_message;
    execute_message.graph_id = read_response.graph_id;
    execute_message.serialized = read_response.response_serialized;

    // read graph need deserialize first
    execute_message.execute_type = ExecuteType::kDeserialize;
    message_hub_.get_executor_queue()->Push(execute_message);

    // read another graph, or do nothing but block
    auto next_graph_id = graph_metadata_info_.GetNextLoadGraphInCurrentRound();
    ReadMessage read_message;
    if (next_graph_id != INVALID_GRAPH_ID) {
      read_message.graph_id = next_graph_id;
      message_hub_.get_reader_queue()->Push(read_message);
    } else {
      // check next round graph which can be read, if not just skip
      auto next_gid_next_round =
          graph_metadata_info_.GetNextLoadGraphInNextRound();
      if (next_gid_next_round != INVALID_GRAPH_ID) {
        read_message.graph_id = next_gid_next_round;
        message_hub_.get_reader_queue()->Push(read_message);
      }
    }
    return true;
  }

  bool ExecuteMessageResponseAndWrite(const Message &resp) {
    // execute finish, to write back the graph
    ExecuteMessage execute_response;
    resp.Get(&execute_response);
    WriteMessage write_message;

    write_message.graph_id = execute_response.graph_id;
    write_message.serializable = execute_response.response_serializable;

    message_hub_.get_writer_queue()->Push(write_message);

    // check border vertex and dependency matrix, mark active subgraph in next
    // round
    // TODO: check border vertex and dependency matrix

    return true;
  }

  bool WriteMessageResponseAndCheckTerminate(const Message &resp) {
    // write finish
    // check if read next round graph
    auto current_round_next_graph_id =
        graph_metadata_info_.GetNextLoadGraphInCurrentRound();
    if (current_round_next_graph_id == INVALID_GRAPH_ID) {
      current_round_++;
      graph_metadata_info_.SyncNextRound();
      auto next_round_first_graph_id =
          graph_metadata_info_.GetNextLoadGraphInCurrentRound();
      if (next_round_first_graph_id == INVALID_GRAPH_ID) {
        // no graph should be loaded, terminate system
        return false;
      } else {
        ReadMessage read_message;
        read_message.graph_id = next_round_first_graph_id;
        message_hub_.get_reader_queue()->Push(read_message);
      }
    }
    return true;
  }

private:
  // graph metadata: graph info, dependency matrix, subgraph metadata, etc.
  data_structures::GraphMetadata graph_metadata_info_;
  int current_round_ = 0;
  // message hub
  MessageHub message_hub_;
  // global message store
  VertexData *global_message_read_;
  VertexData *global_message_write_;
  uint32_t global_message_size_;

  std::unique_ptr<std::thread> thread_;
};

} // namespace sics::graph::core::scheduler

#endif // GRAPH_SYSTEMS_SCHEDULER_H
