#include "miniclean/modules/io_manager.h"

namespace sics::graph::miniclean::modules {

using SerializedGraph =
    sics::graph::miniclean::data_structures::graphs::SerializedMiniCleanGraph;
using Graph = sics::graph::miniclean::data_structures::graphs::MiniCleanGraph;
using GraphID = sics::graph::miniclean::common::GraphID;

IOManager::IOManager(const std::string& data_home,
                     const std::string& graph_home)
    : data_home_(data_home), graph_home_(graph_home) {
  // Load graph metadata.
  YAML::Node metadata_node;
  try {
    metadata_node = YAML::LoadFile(graph_home_ + "partition_result/meta.yaml");
  } catch (YAML::BadFile& e) {
    LOG_FATAL("Read meta.yaml failed. ", e.msg);
  }
  graph_metadata_ = metadata_node.as<GraphMetadata>();
  // Initialize graphs.
  subgraph_state_.resize(graph_metadata_.num_subgraphs, kOnDisk);
  graphs_.resize(graph_metadata_.num_subgraphs);
  serialized_graphs_.resize(graph_metadata_.num_subgraphs);
  current_round_subgraph_pending_.resize(graph_metadata_.num_subgraphs, true);
  subgraph_round_.resize(graph_metadata_.num_subgraphs, 0);
}

void IOManager::SetSubgraphState(const GraphID gid,
                                 const GraphStateType state) {
  // Check whether the state transition is valid.
  switch (state) {
    case kOnDisk:
      if (subgraph_state_.at(gid) != kSerialized) {
        LOGF_FATAL(
            "Cannot set a non-serialized graph {} with state {} to the state: "
            "kOnDisk!",
            gid, state);
      }
      break;
    case kReading:
      if (subgraph_state_.at(gid) != kOnDisk) {
        LOGF_FATAL(
            "Cannot set a non-on-disk graph {} with state {} to the state: "
            "kReading!",
            gid, state);
      }
      break;
    case kSerialized:
      if (subgraph_state_.at(gid) != kReading ||
          subgraph_state_.at(gid) != kComputed) {
        LOGF_FATAL(
            "Cannot set a non-reading or non-computed graph {} with state {} "
            "to the state: kSerialized!",
            gid, state);
      }
      break;
    case kDeserialized:
      if (subgraph_state_.at(gid) != kSerialized) {
        LOGF_FATAL(
            "Cannot set a non-serialized graph {} with state {} to the state: "
            "kDeserialized!",
            gid, state);
      }
      break;
    case kComputed:
      if (subgraph_state_.at(gid) != kDeserialized) {
        LOGF_FATAL(
            "Cannot set a non-deserialized graph {} with state {} to the "
            "state: kComputed!",
            gid, state);
      }
      break;
  }
  subgraph_state_.at(gid) = state;
}

SerializedGraph* IOManager::NewSerializedSubgraph(const GraphID gid) {
  if (subgraph_state_.at(gid) != kOnDisk ||
      serialized_graphs_.at(gid) != nullptr) {
    LOGF_FATAL(
        "Cannot new serialized object for graph {} with state {}. Make sure "
        "that the graph should on the disk and the serialized graph should be "
        "reset to nullptr!",
        gid, subgraph_state_.at(gid));
  }
  serialized_graphs_.at(gid) = std::make_unique<SerializedGraph>();
  return serialized_graphs_.at(gid).get();
}

Graph* IOManager::NewSubgraph(const GraphID gid) {
  if (subgraph_state_.at(gid) != kSerialized || graphs_.at(gid) != nullptr) {
    LOGF_FATAL(
        "Cannot new graph object for graph {} with state {}. Make sure "
        "that the graph should on the disk and the serialized graph should be "
        "reset to nullptr!",
        gid, subgraph_state_.at(gid));
  }
  graphs_.at(gid) = std::make_unique<Graph>(graph_metadata_.subgraphs.at(gid),
                                            graph_metadata_.num_vertices);
  return graphs_.at(gid).get();
}
}  // namespace sics::graph::miniclean::modules