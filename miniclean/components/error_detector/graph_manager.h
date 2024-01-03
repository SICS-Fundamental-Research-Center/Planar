#ifndef MINICLEAN_COMPONENTS_ERROR_DETECTOR_GRAPH_MANAGER_H_
#define MINICLEAN_COMPONENTS_ERROR_DETECTOR_GRAPH_MANAGER_H_

#include <memory>

#include "core/data_structures/serialized.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/graphs/miniclean_graph.h"
#include "miniclean/data_structures/graphs/miniclean_graph_metadata.h"
#include "miniclean/data_structures/graphs/serialized_miniclean_graph.h"

namespace sics::graph::miniclean::components::error_detector {

typedef enum {
  kOnDisk = 0,
  kInMemory,
} GraphStateType;

class GraphManager {
 private:
  using SerializedMiniCleanGraph =
      sics::graph::miniclean::data_structures::graphs::SerializedMiniCleanGraph;
  using Graph = sics::graph::miniclean::data_structures::graphs::MiniCleanGraph;
  using GraphID = sics::graph::miniclean::common::GraphID;

 public:
  explicit GraphManager(const size_t num_subgraphs)
      : num_subgraphs_(num_subgraphs) {
    subgraph_state_.resize(num_subgraphs_, kOnDisk);
    serialized_graphs_.resize(num_subgraphs_);
    graphs_.resize(num_subgraphs_);
  }

  GraphStateType GetSubgraphState(const GraphID gid) const {
    return subgraph_state_.at(gid);
  }

  SerializedMiniCleanGraph* NewSerializedSubgraph(const GraphID gid) {
    serialized_graphs_.at(gid) = std::make_unique<SerializedMiniCleanGraph>();
    return serialized_graphs_.at(gid).get();
  }

  Graph* NewSubgraph(const GraphID gid) {
    
  }

 private:
  const size_t num_subgraphs_;
  std::vector<GraphStateType> subgraph_state_;
  std::vector<std::unique_ptr<SerializedMiniCleanGraph>> serialized_graphs_;
  std::vector<std::unique_ptr<Graph>> graphs_;
};
}  // namespace sics::graph::miniclean::components::error_detector

#endif  // MINICLEAN_COMPONENTS_ERROR_DETECTOR_GRAPH_MANAGER_H_
