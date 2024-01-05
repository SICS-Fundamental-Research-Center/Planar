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
  IOManager(const std::string& data_home, const std::string& graph_home);

  const std::vector<GCR>& GetGCRs() const { return gcrs_; }

  GraphStateType GetSubgraphState(GraphID gid) const {
    return subgraph_state_.at(gid);
  }

  // Construct the subgraph.
  Graph* NewSubgraph(GraphID gid);

  // Destruct the graph object and the graph data in memory.
  void ReleaseSubgraph(GraphID gid);

  // In BSP model, pending subgraphs will be set to `true` when current round is
  // finished.
  void SyncCurrentRoundPending() {
    for (GraphID gid = 0; gid < graph_metadata_.num_subgraphs; ++gid) {
      current_round_subgraph_pending_.at(gid) = true;
    }
  }

  void IncreaseSubgraphRound(GraphID gid) { ++subgraph_round_.at(gid); }

  

 private:
  // Load GCR set.
  void LoadGCRs();

  const std::string& data_home_;
  const std::string& graph_home_;
  GraphMetadata graph_metadata_;
  std::vector<GCR> gcrs_;
  std::vector<GraphStateType> subgraph_state_;
  std::vector<bool> current_round_subgraph_pending_;
  std::vector<size_t> subgraph_round_;
  std::vector<std::unique_ptr<Graph>> graphs_;
};
}  // namespace sics::graph::miniclean::components::error_detector

#endif  // MINICLEAN_COMPONENTS_ERROR_DETECTOR_IO_MANAGER_H_
