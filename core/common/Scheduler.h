//
// Created by Yang Liu on 2023/7/27.
//

#ifndef GRAPH_SYSTEMS_SCHEDULER_H
#define GRAPH_SYSTEMS_SCHEDULER_H

#include "data_structures/graph_metadata.h"

namespace sics::graph::core::common {

class Scheduler {
 private:
  data_structures::GraphMetadata graphMetadata;

 public:
  Scheduler();
  ~Scheduler() {}

  void ReadGraphMetadata(std::string graph_metadata_path) {
    YAML::Node graph_metadata_node;
    graph_metadata_node = YAML::LoadFile(graph_metadata_path);
    this->graphMetadata = graph_metadata_node["GraphMetadata"]
                              .as<data_structures::GraphMetadata>();
  }
};

}  // namespace sics::graph::core::common

#endif  // GRAPH_SYSTEMS_SCHEDULER_H
