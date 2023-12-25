#include "tools/graph_partitioner/partitioner/bfs_based_edgecut.h"

#include <fstream>
#include <iostream>
#include <string>

#include "core/common/bitmap.h"
#include "core/common/multithreading/task.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/util/atomic.h"
#include "core/util/logging.h"
#include "tools/common/data_structures.h"
#include "tools/common/io.h"
#include "tools/graph_partitioner/partitioner/hash_based_edgecut.h"
#include "yaml-cpp/yaml.h"

namespace sics::graph::tools::partitioner {

using Bitmap = sics::graph::core::common::Bitmap;
using ThreadPool = sics::graph::core::common::ThreadPool;
using TaskPackage = sics::graph::core::common::TaskPackage;
using EdgelistMetadata = sics::graph::tools::common::EdgelistMetadata;
using Edge = sics::graph::tools::common::Edge;
using EdgeIndex = sics::graph::miniclean::common::EdgeIndex;
using Vertex = sics::graph::miniclean::data_structures::graphs::MiniCleanVertex;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::atomic::WriteMax;
using sics::graph::core::util::atomic::WriteMin;
using MiniCleanGraphReader = sics::graph::miniclean::io::MiniCleanGraphReader;
using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using MiniCleanGraphMetadata =
    sics::graph::miniclean::data_structures::graphs::MiniCleanGraphMetadata;
using MiniCleanSubgraphMetadata =
    sics::graph::miniclean::data_structures::graphs::MiniCleanSubgraphMetadata;
using SerializedMiniCleanGraph =
    sics::graph::miniclean::data_structures::graphs::SerializedMiniCleanGraph;
using ReadMessage = sics::graph::core::scheduler::ReadMessage;
using GraphFormatConverter = sics::graph::tools::common::GraphFormatConverter;

void BFSBasedEdgeCutPartitioner::RunPartitioner() {
  // Load CSR graph.
  LOG_INFO("Loading the graph");
  MiniCleanGraphReader reader(input_path_);
  YAML::Node node = YAML::LoadFile(input_path_ + "partition_result/meta.yaml");
  MiniCleanGraphMetadata graph_metadata = node.as<MiniCleanGraphMetadata>();
  std::unique_ptr<SerializedMiniCleanGraph> serialized_graph =
      std::make_unique<SerializedMiniCleanGraph>();
  ReadMessage read_message;
  read_message.graph_id = 0;
  read_message.response_serialized = serialized_graph.get();
  reader.Read(&read_message, nullptr);
  MiniCleanGraph graph(graph_metadata.subgraphs[0],
                       graph_metadata.num_vertices);
  ThreadPool thread_pool(1);
  graph.Deserialize(thread_pool, std::move(serialized_graph));
  LOG_INFO("Finished loading and deserializing the graph");

  // BFS-based vertex bucketing.
  LOG_INFO("BFS-based vertex bucketing");
  BFSBasedVertexBucketing(graph_metadata.num_vertices / 128, graph);
  LOG_INFO("Finished BFS-based vertex bucketing");
}

void BFSBasedEdgeCutPartitioner::BFSBasedVertexBucketing(
    size_t minimum_n_vertices_to_partition, const MiniCleanGraph& graph) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();
  task_package.reserve(parallelism);

  // Vertex bucketing.
  Bitmap visited_vertex_bitmap(graph.GetNumVertices());
  std::list<std::list<Vertex>> vertex_bucket_list;
  //  1. Collect vertices from a BFS tree rooted at a vertex with maximum
  //  degree.
  LOG_INFO("Collecting vertices from BFS trees");
  while (graph.GetNumVertices() - visited_vertex_bitmap.Count() >
         minimum_n_vertices_to_partition) {
    // Find the vertex with the maximum degree.
    VertexID max_degree = 0;
    VertexID root_vid = MAX_MINICLEAN_VERTEX_ID;
    for (size_t i = 0; i < parallelism; i++) {
      auto task = std::bind([i, parallelism, &graph, &max_degree, &root_vid,
                             &visited_vertex_bitmap]() {
        for (VertexID j = i; j < graph.GetNumVertices(); j += parallelism) {
          if (visited_vertex_bitmap.GetBit(j)) continue;
          VertexID degree =
              graph.GetInDegreeByLocalID(j) + graph.GetOutDegreeByLocalID(j);
          if (WriteMax(&max_degree, degree)) root_vid = j;
        }
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();
    if (MAX_MINICLEAN_VERTEX_ID == root_vid) break;
    visited_vertex_bitmap.SetBit(root_vid);

    // Collect children rooted at root_vid.
    std::list<VertexID> bfs_queue = {root_vid};
    visited_vertex_bitmap.SetBit(root_vid);
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
        auto task = std::bind([this, i, parallelism, &active_vertices,
                               &visited_vertex_bitmap, &bfs_queue, &graph]() {
          for (VertexID j = i; j < active_vertices.size(); j += parallelism) {
            auto vid = active_vertices.at(j);
            auto vertex = graph.GetVertexByLocalID(vid);
            for (VertexID k = 0; k < vertex.indegree; k++) {
              auto src = vertex.incoming_edges[k];
              if (src == vid) LOG_FATAL("Self-loop detected.");
              std::lock_guard<std::mutex> lock(mtx_);
              if (!visited_vertex_bitmap.GetBit(src)) {
                visited_vertex_bitmap.SetBit(src);
                bfs_queue.push_back(src);
              }
            }
            for (VertexID k = 0; k < vertex.outdegree; k++) {
              auto dst = vertex.outgoing_edges[k];
              if (dst == vid) LOG_FATAL("Self-loop detected.");
              std::lock_guard<std::mutex> lock(mtx_);
              if (!visited_vertex_bitmap.GetBit(dst)) {
                visited_vertex_bitmap.SetBit(dst);
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
    vertex_bucket_list.emplace_back(vertex_bucket);
  }
  //  2. Collect the remaining vertices.
  LOG_INFO("Collecting the remaining vertices");
  std::list<Vertex> bucket_for_remaining_vertices;
  for (size_t i = 0; i < parallelism; i++) {
    auto task = std::bind([this, i, parallelism, &graph, &visited_vertex_bitmap,
                           &bucket_for_remaining_vertices]() {
      for (VertexID j = i; j < graph.GetNumVertices(); j += parallelism) {
        if (visited_vertex_bitmap.GetBit(j)) continue;
        std::lock_guard<std::mutex> lock(mtx_);
        visited_vertex_bitmap.SetBit(j);
        bucket_for_remaining_vertices.emplace_back(graph.GetVertexByLocalID(j));
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  if (!bucket_for_remaining_vertices.empty())
    vertex_bucket_list.emplace_back(bucket_for_remaining_vertices);

  // Redistributing.
  LOG_INFO("Redistributing");
  Redistributing(vertex_bucket_list);

  // Print the graph
  LOG_INFO("Printing the graph");
  for (const auto& bucket : vertex_bucket_list) {
    for (const auto& v : bucket) {
      std::cout << v.global_vid << " ";
    }
    std::cout << std::endl;
    std::cout << "----------" << bucket.size() << "-----------" << std::endl;
  }

  // Write vertex buckets to disk.
  GraphFormatConverter graph_format_converter(output_path_);
  GraphMetadata graph_metadata;
  // graph_metadata.set_num_vertices(visited_vertex_bitmap.Count());
  // graph_metadata.set_num_edges(edges_visited.Count());
  // graph_metadata.set_num_subgraphs(n_partitions_);
  // graph_metadata.set_max_vid(max_vid);
  // graph_metadata.set_min_vid(min_vid);

  // LOG_INFO("Writing the subgraphs to disk");
  // graph_format_converter.WriteSubgraph(vertex_buckets, graph_metadata,
  //                                      store_strategy_);

  // LOG_INFO("Finished writing the subgraphs to disk");
}

void BFSBasedEdgeCutPartitioner::Redistributing(
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
}

}  // namespace sics::graph::tools::partitioner