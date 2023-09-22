#include "tools/graph_partitioner/partitioner/csr_based_planar_vertexcut.h"

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/hash/Hash.h>

#include <algorithm>
#include <filesystem>
#include <string>

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/common/types.h"
#include "core/io/csr_reader.h"
#include "core/util/atomic.h"
#include "core/util/logging.h"
#include "tools/common/data_structures.h"
#include "tools/common/io.h"

namespace sics::graph::tools::partitioner {

using Vertex = sics::graph::core::data_structures::graph::ImmutableCSRVertex;
using sics::graph::core::common::Bitmap;
using sics::graph::core::common::ThreadPool;
using sics::graph::core::common::VertexID;
using sics::graph::core::data_structures::GraphMetadata;
using sics::graph::tools::common::StoreStrategy2Enum;
using std::filesystem::create_directory;
using std::filesystem::exists;
using Edge = sics::graph::tools::common::Edge;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::data_structures::GraphMetadata;
using sics::graph::core::data_structures::SubgraphMetadata;
using sics::graph::core::data_structures::graph::ImmutableCSRGraph;
using sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using sics::graph::core::io::CSRReader;
using sics::graph::core::scheduler::ReadMessage;
using sics::graph::core::scheduler::WriteMessage;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::atomic::WriteMax;
using sics::graph::core::util::atomic::WriteMin;
using sics::graph::tools::common::Edges;
using sics::graph::tools::common::GraphFormatConverter;

void CSRBasedPlanarVertexCutPartitioner::RunPartitioner() {
  LOG_INFO("CSRBasedPlanarVertexCutPartitioner::RunPartitioner()");

  LOG_INFO(input_path_, "meta.yaml");
  auto metadata = YAML::LoadFile(input_path_ + "meta.yaml");
  auto graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();

  ThreadPool thread_pool(1);

  CSRReader csr_reader(input_path_);
  ReadMessage read_message;
  auto serialized_csr = std::make_unique<SerializedImmutableCSRGraph>();
  read_message.response_serialized = serialized_csr.get();
  read_message.graph_id = 0;
  csr_reader.Read(&read_message, nullptr);
  ImmutableCSRGraph serializable_csr(
      graph_metadata.GetSubgraphMetadata(read_message.graph_id));
  serializable_csr.Deserialize(thread_pool, std::move(serialized_csr));
  serializable_csr.ShowGraph();

  auto&& out = SortBFSBranch(
      ceil((double)serializable_csr.get_num_outgoing_edges() / 128.0),
      serializable_csr);

  Redistributing(n_partitions_, &out);

  auto edge_buckets = ConvertListofEdge2Edges(out);

  for (size_t i = 0; i < edge_buckets.size(); i++) {
    auto edges = edge_buckets[i];
    LOG_INFO("num_edges: ", edges.get_metadata().num_edges,
             "num_vertices: ", edges.get_metadata().num_vertices);
  }

  // Write the subgraphs to disk
  GraphFormatConverter graph_format_converter(output_path_);
  GraphMetadata new_graph_metadata;
  new_graph_metadata.set_num_vertices(graph_metadata.get_num_vertices());
  new_graph_metadata.set_num_edges(graph_metadata.get_num_edges());
  new_graph_metadata.set_num_subgraphs(edge_buckets.size());
  new_graph_metadata.set_max_vid(graph_metadata.get_max_vid());
  new_graph_metadata.set_min_vid(graph_metadata.get_min_vid());

  LOG_INFO("Writing the subgraphs to disk");
  graph_format_converter.WriteSubgraph(edge_buckets, new_graph_metadata,
                                       store_strategy_);
}

std::list<std::list<Edge>> CSRBasedPlanarVertexCutPartitioner::SortBFSBranch(
    size_t minimum_n_edges_of_a_branch, const ImmutableCSRGraph& graph) {
  LOG_INFO("SorrBFSBranch, minimum_n_edges_of_branch",
           minimum_n_edges_of_a_branch);
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();
  task_package.reserve(parallelism);

  // Loop until the rest of edges less than the given upper bound.
  Bitmap visited(graph.get_num_vertices());
  Bitmap global_visited(graph.get_max_vid());

  std::list<std::list<Edge>> out;
  size_t count = 0;
  while (graph.get_num_vertices() - visited.Count() >
         minimum_n_edges_of_a_branch) {
    // Find the root whose out degree is maximum in graph.
    VertexID max_degree = 0, root = MAX_VERTEX_ID;
    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([i, parallelism, &graph, &max_degree, &root,
                             &visited]() {
        for (VertexID j = i; j < graph.get_num_vertices(); j += parallelism) {
          if (visited.GetBit(j)) continue;
          if (WriteMax(&max_degree, graph.GetOutDegreeByLocalID(j))) root = j;
        }
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();

    if (MAX_VERTEX_ID == root) {
      break;
    } else {
      visited.SetBit(root);
    }

    // Get the unvisited one hop neighbors of the root and store them at
    // active_vertices_for_each_branch, where each neighbor of root is a new
    // branch.
    auto u = graph.GetVertexByLocalID(root);
    auto active_vertices_for_each_branch =
        new std::list<VertexID>[u.outdegree]();
    auto new_branches = new std::list<Edge>[u.outdegree]();

    size_t res_vertices = u.outdegree;
    for (VertexID i = 0; i < u.outdegree; i++) {
      if (!visited.GetBit(u.outgoing_edges[i])) {
        active_vertices_for_each_branch[i].emplace_back(u.outgoing_edges[i]);
        visited.SetBit(u.outgoing_edges[i]);
      }
      Edge e = {u.vid, u.outgoing_edges[i]};
      new_branches[i].emplace_back(e);
    }

    // BFS traversal, one level at a time.
    LOG_INFO("BFS traversal, one level at a time.");
    std::mutex mtx;
    while (res_vertices != 0) {
      // LOG_INFO("res: ", res_vertices, " ", minimum_n_edges_of_a_branch);
      res_vertices = 0;
      for (unsigned int i = 0; i < parallelism; i++) {
        auto task = std::bind([&, i]() {
          for (VertexID j = i; j < u.outdegree; j += parallelism) {
            if (active_vertices_for_each_branch[j].empty()) continue;
            auto active_vertex_id = active_vertices_for_each_branch[j].front();
            active_vertices_for_each_branch[j].pop_front();
            auto v = graph.GetVertexByLocalID(active_vertex_id);
            for (VertexID k = 0; k < v.outdegree; k++) {
              {
                std::lock_guard<std::mutex> lock(mtx);
                if (!visited.GetBit(v.outgoing_edges[k])) {
                  visited.SetBit(v.outgoing_edges[k]);
                  active_vertices_for_each_branch[j].emplace_back(
                      v.outgoing_edges[k]);
                }
              }
              Edge e = {v.vid, v.outgoing_edges[k]};
              new_branches[j].emplace_back(e);
            }
          }
        });
        task_package.push_back(task);
      }
      thread_pool.SubmitSync(task_package);
      task_package.clear();

      for (VertexID i = 0; i < u.outdegree; i++) {
        res_vertices += active_vertices_for_each_branch[i].size();
      }
    }
    delete[] active_vertices_for_each_branch;
    for (VertexID i = 0; i < u.outdegree; i++) {
      out.emplace_back(new_branches[i]);
      count += new_branches[i].size();
    }
  }

  std::list<Edge> res_branch;
  std::mutex mtx;
  LOG_INFO("num_vertices: ", graph.get_num_vertices(),
           ", COUNT: ", visited.Count());
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i]() {
      for (VertexID j = i; j < graph.get_num_vertices(); j += parallelism) {
        if (visited.GetBit(j)) continue;
        visited.SetBit(j);
        auto res_u = graph.GetVertexByLocalID(j);
        for (size_t k = 0; k < res_u.outdegree; k++) {
          Edge e = {res_u.vid, res_u.outgoing_edges[k]};
          LOG_INFO("emplace", e.src, "->", e.dst);
          {
            std::lock_guard<std::mutex> lock(mtx);
            res_branch.emplace_back(e);
          }
        }
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  count += res_branch.size();
  out.emplace_back(res_branch);

  LOG_INFO("total edges: ", count);
  LOG_INFO("origin branches: ", out.size());
  LOG_INFO("visited Count: ", visited.Count());
  // Sort the branches by the number of edges.
  out.sort([](const auto& l, const auto& r) { return l.size() < r.size(); });
  return out;
}

void CSRBasedPlanarVertexCutPartitioner::Redistributing(
    GraphID expected_n_of_branches,
    std::list<std::list<Edge>>* sorted_list_of_branches) {
  LOG_INFO("Redistribution.");
  VertexID n_new_branches = MAX_VERTEX_ID;
  while (n_new_branches >= expected_n_of_branches * 2) {
    n_new_branches = ceil(sorted_list_of_branches->size() / 2.0);
    LOG_INFO("n_branches: ", n_new_branches,
             " expected branches: ", expected_n_of_branches);
    auto iter_begin = sorted_list_of_branches->begin();
    auto iter_end = sorted_list_of_branches->end();
    iter_end--;

    if (sorted_list_of_branches->size() % 2 == 0) {
      size_t count = 0;
      while (count < n_new_branches) {
        auto branch = sorted_list_of_branches->back();
        sorted_list_of_branches->pop_back();
        iter_begin->splice(iter_begin->end(), branch);
        iter_begin++;
        count++;
      }
    } else {
      size_t count = 0;
      while (count < n_new_branches - 1) {
        auto branch = sorted_list_of_branches->back();
        sorted_list_of_branches->pop_back();
        iter_begin->splice(iter_begin->end(), branch);
        iter_begin++;
        count++;
      }
    }
  }
  sorted_list_of_branches->sort(
      [](const auto& l, const auto& r) { return l.size() < r.size(); });
  while (sorted_list_of_branches->size() > expected_n_of_branches) {
    auto branch = sorted_list_of_branches->back();
    sorted_list_of_branches->pop_back();
    auto iter_rbegin = sorted_list_of_branches->rbegin();
    iter_rbegin->splice(iter_rbegin->end(), branch);
  }
}

std::vector<Edges> CSRBasedPlanarVertexCutPartitioner::ConvertListofEdge2Edges(
    const std::list<std::list<Edge>>& list_of_branches) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();

  task_package.reserve(parallelism);

  auto max_vid_per_edgelist = new VertexID[list_of_branches.size()]();
  auto n_edges_per_edgelist = new size_t[list_of_branches.size()]();

  // Get the metadata of each edgelist.
  size_t br_i = 0;
  for (const auto& branch : list_of_branches) {
    std::for_each(branch.begin(), branch.end(),
                  [&max_vid_per_edgelist, br_i](auto& e) {
                    WriteMax(&max_vid_per_edgelist[br_i], e.src);
                    WriteMax(&max_vid_per_edgelist[br_i], e.dst);
                  });
    br_i++;
  }

  std::vector<Bitmap> visited;
  visited.reserve(list_of_branches.size());
  for (size_t i = 0; i < list_of_branches.size(); i++) {
    Bitmap bm(max_vid_per_edgelist[i]);
    visited.emplace_back(std::move(bm));
  }
  br_i = 0;
  for (const auto& branch : list_of_branches) {
    std::for_each(branch.begin(), branch.end(), [&visited, br_i](auto& e) {
      visited[br_i].SetBit(e.src);
      visited[br_i].SetBit(e.dst);
    });
    n_edges_per_edgelist[br_i] = branch.size();
    br_i++;
  }

  // Construct edgelist.
  std::vector<Edges> vec_edges;
  vec_edges.reserve(list_of_branches.size());
  for (size_t i = 0; i < list_of_branches.size(); i++) {
    LOG_INFO("branch id: ", i, " size: ", visited[i].Count(),
             " max: ", max_vid_per_edgelist[i],
             " n_edges: ", n_edges_per_edgelist[i]);
    Edges edges(
        {visited[i].Count(), n_edges_per_edgelist[i], max_vid_per_edgelist[i]});
    vec_edges.emplace_back(std::move(edges));
  }
  delete[] max_vid_per_edgelist;
  delete[] n_edges_per_edgelist;
  br_i = 0;
  for (const auto& branch : list_of_branches) {
    size_t index = 0;
    auto buffer_edges = vec_edges[br_i].get_base_ptr();
    std::for_each(
        branch.begin(), branch.end(),
        [&index, &buffer_edges, &br_i](auto& e) { buffer_edges[index++] = e; });
    br_i++;
  }

  return vec_edges;
}

}  // namespace sics::graph::tools::partitioner