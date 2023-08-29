#ifndef SICS_GRAPH_SYSTEMS_TOOLS_VERTEXCUT_PARTITIONER_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_VERTEXCUT_PARTITIONER_H_

#include "core/common/types.h"
#include "tools/graph_partitioner/partitioner/partitioner_base.h"

namespace sics::graph::tools::partitioner {

// @DESCRIPTION A vertex-cut partitioning divides edges of a graph into equal
// size clusters. f vertex-cuts are used. A vertex-cut partitioning divides
// edges of a graph into equal size clusters. The vertices that hold the
// endpoints of an edge are also placed in the same cluster as the edge itself.
// However, the vertices are not unique across clusters and might have to be
// replicated, due to the distribution of their edges across different clusters.

// For example, a graph
// G = {V, E}
// V = {v0, v1, v2, v3, v4}
// E = {(v0, v2), (v0, v3), (v1, v0), (v3, v1), (v3, v4), (v4, v1), (v4, v2)}
// is split into F0 that consists of V_F0: {v0, v1, v2, v3}, E_F0: {(v0,
// v2), (v0, v3), (v1, v0)} and F1 that consists of V_F1: {v1, v2, v3, v4},
// E_F1: {(v3, v1), (v3, v4), (v4, v1), (v4, v2)}
// @PARAMETERS
// input_path: Path to the input graph file.
// output_path: Path to the output partition file.
// partitioner: Partitioner to use [edgecut, vertexcut, hybridcut].
// n_partitions: Number of partitions to use.
// store_strategy: StoreStrategy to use [incoming_only, outgoing_only,
// unconstrained], corresponding to store incoming edges only, outgoing edges
// only, and both two respectively.
class VertexCutPartitioner : public PartitionerBase {
 private:
  using StoreStrategy = sics::graph::tools::common::StoreStrategy;
  using VertexID = sics::graph::core::common::VertexID;

 public:
  VertexCutPartitioner(const std::string& input_path,
                       const std::string& output_path,
                       StoreStrategy store_strategy, VertexID n_partitions)
      : PartitionerBase(input_path, output_path, store_strategy),
        n_partitions_(n_partitions) {}

  void RunPartitioner() override;

 private:
  VertexID n_partitions_;

  VertexID GetBucketID(VertexID vid, VertexID n_bucket) const;
};

}  // namespace sics::graph::tools::partitioner

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_VERTEXCUT_PARTITIONER_H_
