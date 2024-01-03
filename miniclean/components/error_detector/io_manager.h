#ifndef MINICLEAN_COMPONENTS_ERROR_DETECTOR_IO_MANAGER_H_
#define MINICLEAN_COMPONENTS_ERROR_DETECTOR_IO_MANAGER_H_

#include <yaml-cpp/yaml.h>

#include <memory>

#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/serialized.h"
#include "core/scheduler/message.h"
#include "core/util/logging.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/gcr/light_gcr.h"
#include "miniclean/data_structures/graphs/miniclean_graph.h"
#include "miniclean/data_structures/graphs/miniclean_graph_metadata.h"
#include "miniclean/data_structures/graphs/serialized_miniclean_graph.h"
#include "miniclean/io/miniclean_graph_reader.h"

namespace sics::graph::miniclean::components::error_detector {

typedef enum {
  kOnDisk = 0,
  kInMemory,
} GraphStateType;

class IOManager {
 private:
  using SerializedGraph =
      sics::graph::miniclean::data_structures::graphs::SerializedMiniCleanGraph;
  using Graph = sics::graph::miniclean::data_structures::graphs::MiniCleanGraph;
  using GraphID = sics::graph::miniclean::common::GraphID;
  using GraphMetadata =
      sics::graph::miniclean::data_structures::graphs::MiniCleanGraphMetadata;
  using Reader = sics::graph::miniclean::io::MiniCleanGraphReader;
  using ReadMessage = sics::graph::core::scheduler::ReadMessage;
  using ThreadPool = sics::graph::core::common::ThreadPool;
  using GCR = sics::graph::miniclean::data_structures::gcr::LightGCR;

 public:
  IOManager() = default;
  explicit IOManager(const std::string& data_home,
                     const std::string& graph_home)
      : data_home_(data_home), graph_home_(graph_home) {
    // Load graph metadata.
    YAML::Node metadata_node;
    try {
      metadata_node =
          YAML::LoadFile(graph_home_ + "partition_result/meta.yaml");
    } catch (YAML::BadFile& e) {
      LOG_FATAL("Read meta.yaml failed. ", e.msg);
    }
    graph_metadata_ = metadata_node.as<GraphMetadata>();
    // Load GCR set.
    LoadGCRs();
    // Initialize graphs.
    subgraph_state_.resize(graph_metadata_.num_subgraphs, kOnDisk);
    graphs_.resize(graph_metadata_.num_subgraphs);
  }

  const std::vector<GCR>& GetGCRs() const { return gcrs_; }

  GraphStateType GetSubgraphState(const GraphID gid) const {
    return subgraph_state_.at(gid);
  }

  // Construct the subgraph.
  Graph* NewSubgraph(const GraphID gid);

  // Destruct the graph object and the graph data in memory.
  void ReleaseSubgraph(const GraphID gid);

 private:
  // Load GCR set.
  void LoadGCRs();

  std::string data_home_;
  std::string graph_home_;
  GraphMetadata graph_metadata_;
  std::vector<GCR> gcrs_;
  std::vector<GraphStateType> subgraph_state_;
  std::vector<std::unique_ptr<Graph>> graphs_;
};
}  // namespace sics::graph::miniclean::components::error_detector

#endif  // MINICLEAN_COMPONENTS_ERROR_DETECTOR_IO_MANAGER_H_
