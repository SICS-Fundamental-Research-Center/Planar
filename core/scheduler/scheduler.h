#ifndef GRAPH_SYSTEMS_SCHEDULER_H
#define GRAPH_SYSTEMS_SCHEDULER_H

#include "data_structures/graph_metadata.h"
#include "message_hub.h"

namespace sics::graph::core::scheduler {

template <typename VertexData, typename EdgeData>
class Scheduler {
 public:
  Scheduler() = default;
  virtual ~Scheduler() = default;

  int GetCurrentRound() const { return current_round_; }

  int GetSubgraphRound(common::GraphID subgraph_gid) const {
    if (graph_metadata_info_.IsSubgraphPendingCurrentRound(subgraph_gid)) {
      return current_round_;
    } else {
      return current_round_ + 1;
    }
  }

  // read graph metadata from meta.yaml file
  // meta file path should be passed by gflags
  void ReadGraphMetadata(const std::string& graph_metadata_path) {
    YAML::Node graph_metadata_node;
    try {
      graph_metadata_node = YAML::LoadFile(graph_metadata_path);
      graph_metadata_info_ = graph_metadata_node["GraphMetadata"]
                                 .as<data_structures::GraphMetadata>();
    } catch (YAML::BadFile& e) {
      LOG_ERROR("meta.yaml file read failed! ", e.msg);
    }
  }

  // get message hub ptr for other component
  MessageHub* get_message_hub() { return &message_hub_; }

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
      while(true){
        Message resp = message_hub_.GetResponse();

        if (resp.is_terminated()) {
          LOG_INFO("*** Scheduler is signaled termination ***");
          break;
        }
        switch (resp.get_type()) {
          case Message::Type::kRead:
            ReadMessageResponse();
            break;
          case Message::Type::kWrite:
            WriteMessageResponse();
            break;
          case Message::Type::kExecute:
            ExecuteMessageResponse();
            break;
          default:
            break;
        }
      }
    });
  }

 private:
  void ReadMessageResponse() {

  }

  void WriteMessageResponse() {

  }

  void ExecuteMessageResponse() {

  }

 private:
  // graph metadata: graph info, dependency matrix, subgraph metadata, etc.
  data_structures::GraphMetadata graph_metadata_info_;
  int current_round_ = 0;
  // message hub
  MessageHub message_hub_;
  // global message store
  VertexData* global_message_read_;
  VertexData* global_message_write_;
  uint32_t global_message_size_;

  std::unique_ptr<std::thread> thread_;
};

}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_SCHEDULER_H
