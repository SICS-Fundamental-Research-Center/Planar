#ifndef SICS_GRAPH_SYSTEMS_TOOLS_BFS_BASED_EDGECUT_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_BFS_BASED_EDGECUT_H_

#include <list>
#include <memory>
#include <vector>

#include "core/common/bitmap.h"
#include "core/common/multithreading/task.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/common/types.h"
#include "core/data_structures/graph/immutable_csr_graph.h"
#include "tools/graph_partitioner/partitioner/partitioner_base.h"

namespace sics::graph::tools::partitioner {

class BFSBasedEdgeCutPartitioner : public PartitionerBase {
 private:
  using StoreStrategy = sics::graph::tools::common::StoreStrategy;
  using VertexID = sics::graph::core::common::VertexID;
  using GraphID = sics::graph::core::common::GraphID;
  using Vertex = sics::graph::core::data_structures::graph::ImmutableCSRVertex;
  using ImmutableCSRGraph =
      sics::graph::core::data_structures::graph::ImmutableCSRGraph;
  using TaskPackage = sics::graph::core::common::TaskPackage;
  using ThreadPool = sics::graph::core::common::ThreadPool;
  using Bitmap = sics::graph::core::common::Bitmap;

 public:
  BFSBasedEdgeCutPartitioner(const std::string& input_path,
                             const std::string& output_path,
                             StoreStrategy store_strategy, GraphID n_partitions,
                             uint64_t max_vertex_num_per_partition)
      : PartitionerBase(input_path, output_path, store_strategy),
        n_partitions_(n_partitions),
        max_vertex_num_per_partition_(max_vertex_num_per_partition) {}

  void RunPartitioner() override;

 private:
  void BFSBasedVertexBucketing(size_t minimum_n_vertices_to_partition);

  VertexID GetUnvisitedVertexWithMaxDegree(Bitmap* visited_vertex_bitmap_ptr);

  void CollectVerticesFromBFSTree(
      VertexID root_vid, std::list<std::list<Vertex>>* vertex_bucket_list_ptr,
      Bitmap* visited_vertex_bitmap_ptr);

  void CollectRemainingVertices(
      std::list<std::list<Vertex>>* vertex_bucket_list_ptr,
      Bitmap* visited_vertex_bitmap_ptr);

  std::vector<std::vector<Vertex>> Redistributing(
      std::list<std::list<Vertex>>& list_of_list);

  const GraphID n_partitions_;
  const uint64_t max_vertex_num_per_partition_;
  ImmutableCSRGraph* graph_ptr_;
  std::unique_ptr<ThreadPool> thread_pool_ptr_;
  TaskPackage task_package_;
  unsigned int parallelism_;
  std::mutex mtx_;
};

}  // namespace sics::graph::tools::partitioner

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_BFS_BASED_EDGECUT_H_