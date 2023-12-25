#ifndef SICS_GRAPH_SYSTEMS_TOOLS_BFS_BASED_EDGECUT_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_BFS_BASED_EDGECUT_H_

#include <list>
#include <memory>
#include <vector>

#include "core/data_structures/graph_metadata.h"
#include "core/scheduler/message.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/graphs/miniclean_graph.h"
#include "miniclean/data_structures/graphs/miniclean_graph_metadata.h"
#include "miniclean/data_structures/graphs/serialized_miniclean_graph.h"
#include "miniclean/io/miniclean_graph_reader.h"
#include "tools/graph_partitioner/partitioner/partitioner_base.h"

namespace sics::graph::tools::partitioner {

class BFSBasedEdgeCutPartitioner : public PartitionerBase {
 private:
  using StoreStrategy = sics::graph::tools::common::StoreStrategy;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using GraphID = sics::graph::miniclean::common::GraphID;
  using Vertex =
      sics::graph::miniclean::data_structures::graphs::MiniCleanVertex;
  using MiniCleanGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanGraph;

 public:
  BFSBasedEdgeCutPartitioner(const std::string& input_path,
                             const std::string& output_path,
                             StoreStrategy store_strategy, GraphID n_partitions)
      : PartitionerBase(input_path, output_path, store_strategy),
        n_partitions_(n_partitions) {}

  void RunPartitioner() override;

 private:
  void BFSBasedVertexBucketing(
      size_t minimum_n_vertices_to_partition, const MiniCleanGraph& graph);

  void Redistributing(std::list<std::list<Vertex>>& list_of_list);

  const GraphID n_partitions_;
  std::mutex mtx_;
};

}  // namespace sics::graph::tools::partitioner

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_BFS_BASED_EDGECUT_H_