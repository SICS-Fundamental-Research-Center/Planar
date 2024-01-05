#ifndef MINICLEAN_MODULES_IO_MANAGER_H_
#define MINICLEAN_MODULES_IO_MANAGER_H_

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

namespace sics::graph::miniclean::modules {

typedef enum {
  kOnDisk = 0,
  kReading,
  kSerialized,
  kDeserialized,
  kComputed,
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

  GraphID GetNumSubgraphs() const { return graph_metadata_.num_subgraphs; }

  GraphStateType GetSubgraphState(GraphID gid) const {
    return subgraph_state_.at(gid);
  }

  size_t GetSubgraphRound(GraphID gid) const { return subgraph_round_.at(gid); }

  Graph* GetSubgraph(GraphID gid) const { return graphs_.at(gid).get(); }

  void SetSubgraphState(GraphID gid, GraphStateType state);

  bool IsCurrentPendingSubgraph(GraphID gid) const {
    return current_round_subgraph_pending_.at(gid);
  }

  SerializedGraph* NewSerializedSubgraph(GraphID gid);

  Graph* NewSubgraph(GraphID gid);

  void ReleaseSerializedSubgraph(GraphID gid) {
    serialized_graphs_.at(gid).reset();
  }

  void ReleaseSubgraph(GraphID gid) { graphs_.at(gid).reset(); }

  // In BSP model, pending subgraphs will be set to `true` when current round is
  // finished.
  void SyncCurrentRoundPending() {
    for (GraphID gid = 0; gid < graph_metadata_.num_subgraphs; ++gid) {
      current_round_subgraph_pending_.at(gid) = true;
    }
  }

  void IncreaseSubgraphRound(GraphID gid) { ++subgraph_round_.at(gid); }

 private:
  const std::string& data_home_;
  const std::string& graph_home_;
  GraphMetadata graph_metadata_;
  std::vector<GraphStateType> subgraph_state_;
  std::vector<bool> current_round_subgraph_pending_;
  std::vector<size_t> subgraph_round_;
  std::vector<std::unique_ptr<SerializedGraph>> serialized_graphs_;
  std::vector<std::unique_ptr<Graph>> graphs_;
};
}  // namespace sics::graph::miniclean::modules

#endif  // MINICLEAN_MODULES_IO_MANAGER_H_
