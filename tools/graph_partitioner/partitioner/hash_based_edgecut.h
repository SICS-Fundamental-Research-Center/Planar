#ifndef SICS_GRAPH_SYSTEMS_TOOLS_EDGECUT_PARTITIONER_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_EDGECUT_PARTITIONER_H_

#include "core/common/types.h"
#include "tools/graph_partitioner/partitioner/partitioner_base.h"

namespace sics::graph::tools::partitioner {

// @DESCRIPTION With edgecut partitioner, each vertex is assigned to a
// fragment. In a fragment, inner vertices are those vertices assigned to it,
// and the outer vertices are the remaining vertices adjacent to some of inner
// vertices. The load strategy defines how to store the adjacency between inner
// and outer vertices.
//
// For example, a graph
// G = {V, E}
// V = {v0, v1, v2, v3, v4}
// E = {(v0, v2), (v0, v3), (v1, v0), (v3, v1), (v3, v4), (v4, v1), (v4, v2)}
// is split into F0 that consists of V_F0: {v0, v1, v2}, E_F0: {(v0,
// v2), (v0, v3), (v1, v0)} and F1 that consists of V_F1: {v3, v4}, E_F1: {(v3,
// v1), (v3, v4), (v4, v1), (v4, v2)}
// @EXAMPLE
// EdgeCutPartitioner edgecut_partitioner(
//          FLAGS_i, FLAGS_o, StoreStrategy2Enum(FLAGS_store_strategy),
//          FLAGS_n_partitions);
//      edgecut_partitioner.RunPartitioner();
// @REFERENCE
// Gonzalez, Joseph E., et al. "{PowerGraph}: Distributed {Graph-Parallel}
// computation on natural graphs." 10th USENIX symposium on operating systems
// design and implementation (OSDI 12). 2012.
class HashBasedEdgeCutPartitioner : public PartitionerBase {
 private:
  using StoreStrategy = sics::graph::tools::common::StoreStrategy;
  using VertexID = sics::graph::core::common::VertexID;
  using GraphID = sics::graph::core::common::GraphID;


 public:
  HashBasedEdgeCutPartitioner(const std::string& input_path,
                              const std::string& output_path,
                              StoreStrategy store_strategy,
                              GraphID n_partitions)
      : PartitionerBase(input_path, output_path, store_strategy),
        n_partitions_(n_partitions) {}

  void RunPartitioner() override;

 private:
  const GraphID n_partitions_;

  VertexID GetBucketID(VertexID vid, VertexID n_bucket,
                       size_t n_vertices) const;
};

}  // namespace sics::graph::tools::partitioner

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_EDGECUT_PARTITIONER_H_
