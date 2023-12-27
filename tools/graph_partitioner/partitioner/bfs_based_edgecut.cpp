#include "tools/graph_partitioner/partitioner/bfs_based_edgecut.h"

#include <fstream>
#include <string>

#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/data_structures/graph_metadata.h"
#include "core/io/csr_reader.h"
#include "core/scheduler/message.h"
#include "core/util/atomic.h"
#include "core/util/logging.h"
#include "tools/common/data_structures.h"
#include "tools/common/io.h"
#include "yaml-cpp/yaml.h"

namespace sics::graph::tools::partitioner {

using Bitmap = sics::graph::core::common::Bitmap;
using ThreadPool = sics::graph::core::common::ThreadPool;
using TaskPackage = sics::graph::core::common::TaskPackage;
using EdgelistMetadata = sics::graph::tools::common::EdgelistMetadata;
using Edge = sics::graph::tools::common::Edge;
using EdgeIndex = sics::graph::core::common::EdgeIndex;
using Vertex = sics::graph::core::data_structures::graph::ImmutableCSRVertex;
using VertexID = sics::graph::core::common::VertexID;
using CSRReader = sics::graph::core::io::CSRReader;
using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using SubgraphMetadata = sics::graph::core::data_structures::SubgraphMetadata;
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using ReadMessage = sics::graph::core::scheduler::ReadMessage;
using GraphFormatConverter = sics::graph::tools::common::GraphFormatConverter;
using ImmutableCSRGraph =
    sics::graph::core::data_structures::graph::ImmutableCSRGraph;

using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::atomic::WriteMax;
using sics::graph::core::util::atomic::WriteMin;

void BFSBasedEdgeCutPartitioner::RunPartitioner() {
  // Load CSR graph.
  LOG_INFO("-----Loading the ImmutableCSRGraph-----");
  CSRReader reader(input_path_);
  YAML::Node node = YAML::LoadFile(input_path_ + "meta.yaml");
  GraphMetadata graph_metadata = node["GraphMetadata"].as<GraphMetadata>();
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_graph =
      std::make_unique<SerializedImmutableCSRGraph>();
  ReadMessage read_message;
  read_message.graph_id = 0;
  read_message.response_serialized = serialized_graph.get();
  reader.Read(&read_message, nullptr);
  LOG_INFO("Finished graph loading.");
  ImmutableCSRGraph graph(graph_metadata.GetSubgraphMetadata(0));
  ThreadPool thread_pool(1);
  graph.Deserialize(thread_pool, std::move(serialized_graph));
  LOG_INFO("Finished graph deserializing.");

  // BFS-based vertex bucketing.
  LOG_INFO("-----BFS-based vertex bucketing-----");
  BFSBasedVertexBucketing(graph_metadata.get_num_vertices() / 128, graph);
  LOG_INFO("Finished BFS-based vertex bucketing");
}

void BFSBasedEdgeCutPartitioner::BFSBasedVertexBucketing(
    size_t minimum_n_vertices_to_partition, const ImmutableCSRGraph& graph) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();
  task_package.reserve(parallelism);

  Bitmap visited_vertex_bitmap(graph.get_num_vertices());
  std::list<std::list<Vertex>> vertex_bucket_list;
  LOG_INFO("Collecting vertices from BFS trees");
  while (graph.get_num_vertices() - visited_vertex_bitmap.Count() >
         minimum_n_vertices_to_partition) {
    // 1. Collect vertices rooted from vertices with the maximum degree.
    //   1.1 Find the vertex with the maximum degree.
    VertexID root_vid = GetUnvisitedVertexWithMaxDegree(
        graph, task_package, thread_pool, parallelism, &visited_vertex_bitmap);
    if (MAX_VERTEX_ID == root_vid) break;

    //   1.2 Collect children rooted at root_vid.
    CollectVerticesFromBFSTree(graph, task_package, thread_pool, parallelism,
                               root_vid, &vertex_bucket_list,
                               &visited_vertex_bitmap);
  }
  // 2. Collect the remaining vertices.
  CollectRemainingVertices(graph, task_package, thread_pool, parallelism,
                           &vertex_bucket_list, &visited_vertex_bitmap);
  // 3. Redistribute the vertices.
  std::vector<std::vector<Vertex>> vertex_buckets =
      Redistributing(vertex_bucket_list);
  // 4. Write vertex buckets to disk.
  LOG_INFO("Writing the subgraphs to disk");
  GraphFormatConverter graph_format_converter(output_path_);
  GraphMetadata graph_metadata;
  if (visited_vertex_bitmap.Count() != graph.get_num_vertices())
    LOG_FATAL(
        "The number of vertices in the graph is not equal to the number "
        "of vertices in the vertex buckets.");
  graph_metadata.set_num_vertices(graph.get_num_vertices());
  graph_metadata.set_num_edges(graph.get_num_outgoing_edges());
  graph_metadata.set_num_subgraphs(n_partitions_);
  graph_metadata.set_max_vid(graph.get_max_vid());
  graph_metadata.set_min_vid(graph.get_min_vid());

  graph_format_converter.WriteSubgraph(vertex_buckets, graph_metadata,
                                       store_strategy_);

  LOG_INFO("Finished writing the subgraphs to disk");
}

VertexID BFSBasedEdgeCutPartitioner::GetUnvisitedVertexWithMaxDegree(
    const ImmutableCSRGraph& graph, TaskPackage& task_package,
    ThreadPool& thread_pool, unsigned int parallelism,
    Bitmap* visited_vertex_bitmap_ptr) {
  // Find the vertex with the maximum degree.
  VertexID max_degree = 0;
  VertexID root_vid = MAX_VERTEX_ID;
  for (size_t i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, visited_vertex_bitmap_ptr, &graph,
                           &max_degree, &root_vid]() {
      for (VertexID j = i; j < graph.get_num_vertices(); j += parallelism) {
        if (visited_vertex_bitmap_ptr->GetBit(j)) continue;
        VertexID degree =
            graph.GetInDegreeByLocalID(j) + graph.GetOutDegreeByLocalID(j);
        if (WriteMax(&max_degree, degree)) root_vid = j;
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  if (MAX_VERTEX_ID != root_vid) visited_vertex_bitmap_ptr->SetBit(root_vid);
  return root_vid;
}

void BFSBasedEdgeCutPartitioner::CollectVerticesFromBFSTree(
    const ImmutableCSRGraph& graph, TaskPackage& task_package,
    ThreadPool& thread_pool, unsigned int parallelism, VertexID root_vid,
    std::list<std::list<Vertex>>* vertex_bucket_list_ptr,
    Bitmap* visited_vertex_bitmap_ptr) {
  std::list<VertexID> bfs_queue = {root_vid};
  std::list<Vertex> vertex_bucket;
  while (!bfs_queue.empty()) {
    std::vector<VertexID> active_vertices;
    active_vertices.reserve(bfs_queue.size());
    for (const auto& vid : bfs_queue) {
      active_vertices.push_back(vid);
      vertex_bucket.push_back(graph.GetVertexByLocalID(vid));
    }
    bfs_queue.clear();
    for (size_t i = 0; i < parallelism; i++) {
      auto task = std::bind([this, i, parallelism, visited_vertex_bitmap_ptr,
                             &active_vertices, &bfs_queue, &graph]() {
        for (VertexID j = i; j < active_vertices.size(); j += parallelism) {
          auto vid = active_vertices.at(j);
          auto vertex = graph.GetVertexByLocalID(vid);
          for (VertexID k = 0; k < vertex.indegree; k++) {
            auto src = vertex.incoming_edges[k];
            if (src == vid) LOG_FATAL("Self-loop detected.");
            std::lock_guard<std::mutex> lock(mtx_);
            if (!visited_vertex_bitmap_ptr->GetBit(src)) {
              visited_vertex_bitmap_ptr->SetBit(src);
              bfs_queue.push_back(src);
            }
          }
          for (VertexID k = 0; k < vertex.outdegree; k++) {
            auto dst = vertex.outgoing_edges[k];
            if (dst == vid) LOG_FATAL("Self-loop detected.");
            std::lock_guard<std::mutex> lock(mtx_);
            if (!visited_vertex_bitmap_ptr->GetBit(dst)) {
              visited_vertex_bitmap_ptr->SetBit(dst);
              bfs_queue.push_back(dst);
            }
          }
        }
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();
  }
  vertex_bucket_list_ptr->emplace_back(vertex_bucket);
}

void BFSBasedEdgeCutPartitioner::CollectRemainingVertices(
    const ImmutableCSRGraph& graph, TaskPackage& task_package,
    ThreadPool& thread_pool, unsigned int parallelism,
    std::list<std::list<Vertex>>* vertex_bucket_list_ptr,
    Bitmap* visited_vertex_bitmap_ptr) {
  LOG_INFO("Collecting the remaining vertices");
  std::list<Vertex> bucket_for_remaining_vertices;
  for (size_t i = 0; i < parallelism; i++) {
    auto task = std::bind([this, i, parallelism, visited_vertex_bitmap_ptr,
                           &graph, &bucket_for_remaining_vertices]() {
      for (VertexID j = i; j < graph.get_num_vertices(); j += parallelism) {
        if (visited_vertex_bitmap_ptr->GetBit(j)) continue;
        std::lock_guard<std::mutex> lock(mtx_);
        visited_vertex_bitmap_ptr->SetBit(j);
        bucket_for_remaining_vertices.emplace_back(graph.GetVertexByLocalID(j));
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  if (!bucket_for_remaining_vertices.empty())
    vertex_bucket_list_ptr->emplace_back(bucket_for_remaining_vertices);
}

std::vector<std::vector<Vertex>> BFSBasedEdgeCutPartitioner::Redistributing(
    std::list<std::list<Vertex>>& list_of_list) {
  list_of_list.sort(
      [](const auto& l, const auto& r) { return l.size() < r.size(); });

  while (list_of_list.size() >= n_partitions_ * 2) {
    size_t count = 0;
    size_t max_count = list_of_list.size() / 2;
    if (list_of_list.size() % 2 == 1) max_count -= 1;
    auto iter_begin = list_of_list.begin();
    while (count < max_count) {
      auto back = list_of_list.back();
      list_of_list.pop_back();
      iter_begin->splice(iter_begin->end(), std::move(back));
      count++;
      iter_begin++;
    }
    list_of_list.sort(
        [](const auto& l, const auto& r) { return l.size() < r.size(); });
  }

  auto iter_begin = list_of_list.begin();
  while (list_of_list.size() > n_partitions_) {
    auto back = list_of_list.back();
    list_of_list.pop_back();
    iter_begin->splice(iter_begin->end(), std::move(back));
    iter_begin++;
  }

  std::vector<std::vector<Vertex>> vector_of_vector;
  vector_of_vector.reserve(n_partitions_);

  for (auto& bucket : list_of_list) {
    bucket.sort([](const auto& l, const auto& r) { return l.vid < r.vid; });
    vector_of_vector.emplace_back(bucket.begin(), bucket.end());
  }

  return vector_of_vector;
}

}  // namespace sics::graph::tools::partitioner