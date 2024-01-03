#ifndef MINICLEAN_COMPONENTS_ERROR_DETECTOR_IO_MANAGER_H_
#define MINICLEAN_COMPONENTS_ERROR_DETECTOR_IO_MANAGER_H_

#include <yaml-cpp/yaml.h>

#include <memory>

#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/serialized.h"
#include "core/scheduler/message.h"
#include "core/util/logging.h"
#include "miniclean/common/types.h"
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

 public:
  explicit IOManager(const size_t num_subgraphs, const std::string& graph_home)
      : num_subgraphs_(num_subgraphs), graph_home_(graph_home) {
    subgraph_state_.resize(num_subgraphs_, kOnDisk);
    serialized_graphs_.resize(num_subgraphs_);
    graphs_.resize(num_subgraphs_);
    YAML::Node metadata_node;
    try {
      metadata_node =
          YAML::LoadFile(graph_home_ + "partition_result/meta.yaml");
    } catch (YAML::BadFile& e) {
      LOG_FATAL("Read meta.yaml failed. ", e.msg);
    }
    graph_metadata_ = metadata_node.as<GraphMetadata>();
  }

  GraphStateType GetSubgraphState(const GraphID gid) const {
    return subgraph_state_.at(gid);
  }

  // Construct the subgraph.
  Graph* NewSubgraph(const GraphID gid);

  // Destruct the graph object and the graph data in memory.
  void ReleaseSubgraph(const GraphID gid);

 private:
  const size_t num_subgraphs_;
  const std::string graph_home_;
  GraphMetadata graph_metadata_;
  std::vector<GraphStateType> subgraph_state_;
  std::vector<std::unique_ptr<SerializedGraph>> serialized_graphs_;
  std::vector<std::unique_ptr<Graph>> graphs_;
};
}  // namespace sics::graph::miniclean::components::error_detector

#endif  // MINICLEAN_COMPONENTS_ERROR_DETECTOR_IO_MANAGER_H_
