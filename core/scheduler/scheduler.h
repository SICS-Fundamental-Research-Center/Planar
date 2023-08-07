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

  int GetSubgraphRound(common::GraphID subgraph_gid) const;

  // read graph metadata from meta.yaml file
  // meta file path should be passed by gflags
  void ReadGraphMetadata(const std::string& graph_metadata_path);

  // message part


 private:
  data_structures::GraphMetadata graph_metadata_info_;
  int current_round_ = 0;

  MessageHub message_hub_;

  VertexData* global_message_read_;
  VertexData* global_message_write_;
  uint32_t global_message_size_;

};

}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_SCHEDULER_H
