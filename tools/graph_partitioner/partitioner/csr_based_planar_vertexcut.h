#ifndef SICS_GRAPH_SYSTEMS_TOOLS_PLANAR_VERTEXCUT_CSR_BASED_PARTITIONER_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_PLANAR_VERTEXCUT_CSR_BASED_PARTITIONER_H_

#include <vector>

#include "core/common/bitmap.h"
#include "core/common/types.h"
#include "core/data_structures/graph/immutable_csr_graph.h"
#include "tools/common/data_structures.h"
#include "tools/common/yaml_config.h"
#include "tools/graph_partitioner/partitioner/partitioner_base.h"

namespace sics::graph::tools::partitioner {

class CSRBasedPlanarVertexCutPartitioner : public PartitionerBase {
 private:
  using StoreStrategy = sics::graph::tools::common::StoreStrategy;
  using Edges = sics::graph::tools::common::Edges;
  using Edge = sics::graph::tools::common::Edge;
  using Bitmap = sics::graph::core::common::Bitmap;
  using VertexID = sics::graph::core::common::VertexID;
  using GraphID = sics::graph::core::common::GraphID;
  using EdgelistMetadata = sics::graph::tools::common::EdgelistMetadata;
  using ImmutableCSRGraph =
      sics::graph::core::data_structures::graph::ImmutableCSRGraph;

  std::list<VertexID> members;

 public:
  CSRBasedPlanarVertexCutPartitioner(const std::string& input_path,
                                     const std::string& output_path,
                                     StoreStrategy store_strategy,
                                     VertexID n_partitions)
      : PartitionerBase(input_path, output_path, store_strategy),
        n_partitions_(n_partitions) {}

  void RunPartitioner() override;

  void RunPartitioner(bool biggraph);

 private:
  const GraphID n_partitions_;

  // @DESCRIPTION
  // Decompose the largest branch into a disjoint set of branches. It returns
  // the number of new branches and corresponding branches data as well.
  // @ASSUMPTION
  // We require that vid of inputted graph have been compacted. i.e.
  // the global id of vertex equals to its index and there is no gap between two
  // vertex id.
  // @PARAMETERS
  // minimum_n_edges_of_a_branch: Minimum number of edges in a branch.
  // ImmutableCSRGraph: The graph to be partitioned.
  std::list<std::list<Edge>> SortBFSBranch(size_t minimum_n_edges_of_a_branch,
                                           const ImmutableCSRGraph& graph);

  // @DESCRIPTION
  // Decompose the largest branch into a disjoint set of branches. The different
  // is that it turns to hash-based vertex-cut partitioner when there are too
  // many of unvisited edges in a BFS iteration.
  // @PARAMETERS
  // minimum_n_edges_of_a_branch: Minimum number of edges in a branch.
  // ImmutableCSRGraph: The graph to be partitioned.
  std::list<std::list<Edge>> BigGraphSortBFSBranch(
      size_t minimum_n_edges_of_a_branch, const ImmutableCSRGraph& graph);

  // @DESCRIPTION
  // Redistributing() aim at (1) merging small branches into one and (2)
  // redistributing border vertices.
  // @PARAMETERS
  // expected_n_edges: The number of branches expected to be in the final
  // output.
  // list_of_branches: List of branches.
  // @RETURNS
  // merged_branches: List of merged branches.
  void Redistributing(GraphID expected_n_branches,
                      std::list<std::list<Edge>>* list_of_branches);

  // @DESCRIPTION
  // ConvertListofEdge2Edges() convert list of list of edges to Edges.
  // @PARAMETERS
  // list_of_branches: List of branches.
  // @RETURNS
  // a vector of Edges instances, .i.e. Edgelist Graph.
  std::vector<Edges> ConvertListofEdge2Edges(
      const std::list<std::list<Edge>>& list_of_branches);
};

}  // namespace sics::graph::tools::partitioner

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_PLANAR_VERTEXCUT_PARTITIONER_H_
