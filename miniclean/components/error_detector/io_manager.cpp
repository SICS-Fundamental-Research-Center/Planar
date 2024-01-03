#include "miniclean/components/error_detector/io_manager.h"

namespace sics::graph::miniclean::components::error_detector {

using SerializedGraph =
    sics::graph::miniclean::data_structures::graphs::SerializedMiniCleanGraph;
using Graph = sics::graph::miniclean::data_structures::graphs::MiniCleanGraph;
using GraphID = sics::graph::miniclean::common::GraphID;

Graph* IOManager::NewSubgraph(const GraphID gid) {
  if (subgraph_state_.at(gid) == kInMemory || graphs_.at(gid) != nullptr) {
    LOG_FATAL(
        "Release the previous subgraph object and the graph data before "
        "creating a new subgraph!");
  }
  std::unique_ptr<SerializedGraph> serialized_graph =
      std::make_unique<SerializedGraph>();
  graphs_.at(gid) = std::make_unique<Graph>(graph_metadata_.subgraphs.at(gid),
                                            graph_metadata_.num_vertices);
  // Load graph data.
  Reader reader(graph_home_);
  ReadMessage read_message;
  read_message.graph_id = gid;
  read_message.response_serialized = serialized_graph.get();
  reader.Read(&read_message, nullptr);
  // Deserialize the graph data.
  ThreadPool thread_pool(1);
  graphs_.at(gid)->Deserialize(thread_pool, std::move(serialized_graph));
  subgraph_state_.at(gid) = kInMemory;
  return graphs_.at(gid).get();
}

void IOManager::ReleaseSubgraph(const GraphID gid) {
  if (subgraph_state_.at(gid) == kOnDisk || graphs_.at(gid) == nullptr) {
    LOG_FATAL(
        "The subgraph object or the graph data to be released is nullptr!");
  }
  graphs_.at(gid).reset();
  subgraph_state_.at(gid) = kOnDisk;
}

void IOManager::LoadGCRs() {
  YAML::Node gcrs_node;
  try {
    gcrs_node = YAML::LoadFile(data_home_ + "gcrs.yaml");
  } catch (YAML::BadFile& e) {
    LOG_FATAL("gcrs.yaml file read failed! ", e.msg);
  }
  gcrs_ = gcrs_node["GCRs"].as<std::vector<GCR>>();
}

}  // namespace sics::graph::miniclean::components::error_detector