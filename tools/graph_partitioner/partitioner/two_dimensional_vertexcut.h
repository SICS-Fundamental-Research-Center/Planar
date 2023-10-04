#ifndef SICS_GRAPH_SYSTEMS_TOOLS_2D_VERTEXCUT_PARTITIONER_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_2D_VERTEXCUT_PARTITIONER_H_

#include "core/common/types.h"
#include "tools/graph_partitioner/partitioner/partitioner_base.h"

namespace sics::graph::tools::partitioner {

// @DESCRIPTION Two dimensional vertex cut partitioning divides edges of a graph
// into grids. The partitioner was used in GridGraph, vertices are partitioned
// into 1D chunks and edges are partitioned into 2D blocks according to the
// source and destination vertices.
// @EXAMPLE
//  VertexCutPartitioner vertexcut_partitioner(
//     FLAGS_i, FLAGS_o, StoreStrategy2Enum(FLAGS_store_strategy),
//     FLAGS_n_partitions);
// vertexcut_partitioner.RunPartitioner();
// @REFERENCE
// 2022. Apache Spark Partition Strategy. Retrieved May 1, 2023 from
// https://spark.apache.org/docs/1.4.0/api/java/org/apache/spark/graphx/
// PartitionStrategy.RandomVertexCut$.html
class TwoDimensionalVertexCutPartitioner : public PartitionerBase {
 private:
  using StoreStrategy = sics::graph::tools::common::StoreStrategy;
  using VertexID = sics::graph::core::common::VertexID;
  using GraphID = sics::graph::core::common::GraphID;

 public:
  TwoDimensionalVertexCutPartitioner(const std::string& input_path,
                                     const std::string& output_path,
                                     StoreStrategy store_strategy,
                                     GraphID n_partitions)
      : PartitionerBase(input_path, output_path, store_strategy),
        n_partitions_(n_partitions) {}

  void RunPartitioner() override;

 private:
  GraphID n_partitions_;

  VertexID GetBucketID(VertexID vid, VertexID n_bucket,
                       size_t n_vertices) const;
};

}  // namespace sics::graph::tools::partitioner

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_VERTEXCUT_PARTITIONER_H_
