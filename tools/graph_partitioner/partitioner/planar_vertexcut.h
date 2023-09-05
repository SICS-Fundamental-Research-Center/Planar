#ifndef SICS_GRAPH_SYSTEMS_TOOLS_PLANAR_VERTEXCUT_PARTITIONER_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_PLANAR_VERTEXCUT_PARTITIONER_H_

#include <vector>

#include "core/common/types.h"
#include "tools/common/data_structures.h"
#include "tools/common/yaml_config.h"
#include "tools/graph_partitioner/partitioner/partitioner_base.h"

namespace sics::graph::tools::partitioner {

class PlanarVertexCutPartitioner : public PartitionerBase {
 private:
  using StoreStrategy = sics::graph::tools::common::StoreStrategy;
  using Edges = sics::graph::tools::common::Edges;
  using VertexID = sics::graph::core::common::VertexID;
  using EdgelistMetadata = sics::graph::tools::common::EdgelistMetadata;

 public:
  PlanarVertexCutPartitioner(const std::string& input_path,
                             const std::string& output_path,
                             StoreStrategy store_strategy,
                             VertexID n_partitions)
      : PartitionerBase(input_path, output_path, store_strategy),
        n_partitions_(n_partitions) {}

  void RunPartitioner() override;

 private:
  const uint8_t n_partitions_;

  // @DESCRIPTION
  // Decompose the largest branch into a disjoint set of branches, It returns
  // the number of new branches and corresponding branches data as well.
  std::list<Edges*> SortBFSBranch(Edges& edges, VertexID r);
};

}  // namespace sics::graph::tools::partitioner

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_PLANAR_VERTEXCUT_PARTITIONER_H_
