#include "tools/graph_partitioner/partitioner/csr_based_planar_vertexcut.h"

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/hash/Hash.h>

#include <algorithm>
#include <filesystem>
#include <string>

#include "core/common/multithreading/thread_pool.h"
#include "core/common/types.h"
#include "core/io/csr_reader.h"
#include "core/util/atomic.h"
#include "core/util/logging.h"
#include "tools/common/data_structures.h"
#include "tools/common/io.h"

#ifdef TBB_FOUND
#include <execution>
#endif

#include "sys/types.h"

namespace sics::graph::tools::partitioner {

using Vertex = sics::graph::core::data_structures::graph::ImmutableCSRVertex;
using folly::hash::fnv64_append_byte;
using sics::graph::core::common::Bitmap;
using sics::graph::core::common::EdgeIndex;
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
  // TODO (hsiaoko):
}

void CSRBasedPlanarVertexCutPartitioner::RunPartitioner(bool biggraph) {
  LOG_INFO("CSRBasedPlanarVertexCutPartitioner::RunPartitioner()");

  LOG_INFO(input_path_, "meta.yaml");
  auto metadata = YAML::LoadFile(input_path_ + "meta.yaml");
  auto graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();

  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(1);
  auto task_package = TaskPackage();

  // Read graph.
  CSRReader csr_reader(input_path_);
  ReadMessage read_message;
  read_message.graph_id = 0;
  auto serialized_csr = std::make_unique<SerializedImmutableCSRGraph>();
  read_message.response_serialized = serialized_csr.get();
  csr_reader.Read(&read_message, nullptr);
  ImmutableCSRGraph serializable_csr(
      graph_metadata.GetSubgraphMetadata(read_message.graph_id));
  serializable_csr.Deserialize(thread_pool, std::move(serialized_csr));
  serializable_csr.ShowGraph();

  std::list<std::list<Edge>> out;
  if (biggraph) {
    out = BigGraphSortBFSBranch(
        ceil((double)serializable_csr.get_num_outgoing_edges() / 64),
        serializable_csr);
  } else {
    out = SortBFSBranch(
        ceil((double)serializable_csr.get_num_outgoing_edges() / 64),
        serializable_csr);
  }
  Redistributing(n_partitions_, &out);
  auto&& edge_buckets = ConvertListofEdge2Edges(out);

  // Set metadata of partitioned graph.
  GraphFormatConverter graph_format_converter(output_path_);
  GraphMetadata new_graph_metadata;
  new_graph_metadata.set_num_vertices(graph_metadata.get_num_vertices());
  new_graph_metadata.set_num_edges(graph_metadata.get_num_edges());
  new_graph_metadata.set_max_vid(graph_metadata.get_max_vid());
  new_graph_metadata.set_min_vid(graph_metadata.get_min_vid());
  new_graph_metadata.set_num_subgraphs(edge_buckets.size());

  // Write the subgraphs to disk
  graph_format_converter.WriteSubgraph(edge_buckets, new_graph_metadata,
                                       store_strategy_);
}

std::list<std::list<Edge>> CSRBasedPlanarVertexCutPartitioner::SortBFSBranch(
    size_t minimum_n_edges_of_a_branch, const ImmutableCSRGraph& graph) {
  LOG_INFO("SortBFSBranch, minimum_n_edges_of_branch",
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

    if (MAX_VERTEX_ID == root)
      break;
    else
      visited.SetBit(root);

    // Get the unvisited one hop neighbors of the root and store them at
    // active_vertices_for_each_branch, where each neighbor of root is a new
    // branch.
    auto u = graph.GetVertexByLocalID(root);
    auto active_vertices_for_each_branch =
        new std::list<VertexID>[u.outdegree]();
    auto new_branches = new std::list<Edge>[u.outdegree]();

    size_t res_vertices = u.outdegree;
    // emplace the root of each branch.
    std::mutex mtx;
    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([&, i]() {
        for (VertexID j = i; j < u.outdegree; j += parallelism) {
          if (!visited.GetBit(u.outgoing_edges[j])) {
            visited.SetBit(u.outgoing_edges[j]);
            active_vertices_for_each_branch[j].emplace_back(
                u.outgoing_edges[j]);
          }
          Edge e = {u.vid, u.outgoing_edges[j]};
          new_branches[j].emplace_back(e);
        }
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();

    // BFS traversal, one level at a time.
    LOG_INFO("BFS traversal, one level at a time.");
    while (res_vertices != 0) {
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

      for (unsigned int i = 0; i < parallelism; i++) {
        auto task = std::bind([&, i]() {
          for (VertexID j = i; j < u.outdegree; j += parallelism) {
            WriteAdd(&res_vertices, active_vertices_for_each_branch[j].size());
          }
        });
        task_package.push_back(task);
      }
      thread_pool.SubmitSync(task_package);
      task_package.clear();
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

std::list<std::list<Edge>>
CSRBasedPlanarVertexCutPartitioner::BigGraphSortBFSBranch(
    size_t minimum_n_edges_of_a_branch, const ImmutableCSRGraph& graph) {
  LOG_INFO("BigGraphSortBFSBranch, minimum_n_edges_of_branch",
           minimum_n_edges_of_a_branch);
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();
  task_package.reserve(parallelism);
  std::mutex mtx;

  VertexID n_branches =
      parallelism > n_partitions_ * 2 ? parallelism : n_partitions_ * 2;

  auto branch_id_for_each_edge = new VertexID[graph.get_num_outgoing_edges()]();
  Bitmap edge_visited(graph.get_num_outgoing_edges());
  Bitmap vertex_visited(graph.get_num_vertices());

  std::list<std::list<Edge>> out;
  size_t count = 0;

  // Vertex visited_times_of_vertex[graph.get_num_vertices()]();
  Bitmap no_empty_branch(n_branches);
  // Loop until the rest of edges less than the given upper bound.
  while (edge_visited.Count() != graph.get_num_outgoing_edges()) {
    VertexID max_degree = 0, root = MAX_VERTEX_ID;
    LOG_INFO("Number of visited vertices: ", vertex_visited.Count(), " / ",
             graph.get_num_vertices(),
             " number of visited edges: ", edge_visited.Count(), " / ",
             graph.get_num_outgoing_edges());
    size_t visited_edge_count = edge_visited.Count();
    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([i, parallelism, &graph, &max_degree, &root,
                             &vertex_visited]() {
        for (VertexID j = i; j < graph.get_num_vertices(); j += parallelism) {
          if (vertex_visited.GetBit(j)) continue;
          auto u = graph.GetVertexByLocalID(j);
          if (WriteMax(&max_degree, graph.GetOutDegreeByLocalID(j))) root = j;
        }
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();

    if (MAX_VERTEX_ID == root)
      break;
    else
      vertex_visited.SetBit(root);

    Bitmap in_frontier(graph.get_num_vertices());
    Bitmap out_frontier(graph.get_num_vertices());

    // Get the unvisited one hop neighbors of the root and store them at
    // out_frontier, where each neighbor of root is a new
    // branch.
    auto u = graph.GetVertexByLocalID(root);
    LOG_INFO("root: ", u.vid, " outdegree: ", u.outdegree);

    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([&, i]() {
        for (VertexID j = i; j < u.outdegree; j += parallelism) {
          if (!vertex_visited.GetBit(u.outgoing_edges[j])) {
            vertex_visited.SetBit(u.outgoing_edges[j]);
            in_frontier.SetBit(u.outgoing_edges[j]);
          }
          auto e_offset = graph.GetOutOffsetByLocalID(u.vid) + j;
          edge_visited.SetBit(e_offset);
          branch_id_for_each_edge[e_offset] = j % n_branches;
        }
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();

    LOG_INFO(in_frontier.Count(), " vertices in frontier.");
    // BFS traversal, one level at a time.
    while (in_frontier.Count() > 0) {
      for (unsigned int i = 0; i < parallelism; i++) {
        auto task = std::bind([&, i]() {
          for (VertexID j = i; j < graph.get_num_vertices(); j += parallelism) {
            if (!in_frontier.GetBit(j)) continue;
            auto v = graph.GetVertexByLocalID(j);
            for (VertexID k = 0; k < v.outdegree; k++) {
              if (!vertex_visited.GetBit(v.outgoing_edges[k])) {
                out_frontier.SetBit(v.outgoing_edges[k]);
                vertex_visited.SetBit(v.outgoing_edges[k]);
              }
              auto e_offset = graph.GetOutOffsetByLocalID(v.vid) + k;
              branch_id_for_each_edge[e_offset] = j % n_branches;
              edge_visited.SetBit(e_offset);
            }
          }
        });
        task_package.push_back(task);
      }
      thread_pool.SubmitSync(task_package);
      task_package.clear();
      in_frontier.Clear();
      LOG_INFO("Active: ", out_frontier.Count());
      std::swap(in_frontier, out_frontier);
    }

    if (graph.get_num_outgoing_edges() - edge_visited.Count() <
        minimum_n_edges_of_a_branch)
      break;
    LOG_INFO("New visited edges: ", edge_visited.Count() - visited_edge_count,
             " / ", graph.get_num_outgoing_edges() * 0.001);
    if (edge_visited.Count() - visited_edge_count <
        (graph.get_num_outgoing_edges() * 0.001))
      break;
  }

  // Construct the new graph by merge the res of edges.
  LOG_INFO("Construct the new graph by merge the res of edges.  res edges: ",
           graph.get_num_outgoing_edges() - edge_visited.Count());
  auto out_edges = graph.GetOutgoingEdgesBasePointer();
  for (unsigned int tid = 0; tid < parallelism; tid++) {
    auto task = std::bind([&, tid]() {
      for (VertexID j = tid; j < graph.get_num_vertices(); j += parallelism) {
        auto u_offset = graph.GetOutOffsetByLocalID(j);
        auto u = graph.GetVertexByLocalID(j);
        for (VertexID k = 0; k < u.outdegree; k += parallelism) {
          if (edge_visited.GetBit(u_offset + k)) continue;
          auto branch_id = fnv64_append_byte(u.vid * 133221, 3) % n_branches;
          if (branch_id == 0) branch_id = u.outgoing_edges[k] % n_branches;
          branch_id_for_each_edge[u_offset + k] = branch_id;
          edge_visited.SetBit(u_offset + k);
        }
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  std::vector<std::list<Edge>> vec_branches;
  vec_branches.resize(n_branches);
  std::mutex mtx_branches[n_branches];
  for (unsigned int tid = 0; tid < parallelism; tid++) {
    auto task = std::bind([&, tid]() {
      for (VertexID j = tid; j < graph.get_num_vertices(); j += parallelism) {
        auto u_offset = graph.GetOutOffsetByLocalID(j);
        auto u = graph.GetVertexByLocalID(j);
        for (VertexID k = 0; k < u.outdegree; k++) {
          auto bid = branch_id_for_each_edge[u_offset + k];
          {
            std::lock_guard<std::mutex> ltx(mtx_branches[bid]);
            vec_branches[bid].emplace_back(Edge(u.vid, u.outgoing_edges[k]));
          }
        }
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  size_t total_visited_edges = 0;
  for (size_t i = 0; i < vec_branches.size(); i++) {
    if (vec_branches[i].size() > 0) {
      LOG_INFO("Branch ", i, " has ", vec_branches[i].size(), " edges.");
      total_visited_edges += vec_branches[i].size();
      out.emplace_back(std::move(vec_branches[i]));
    }
  }

  LOG_INFO("Total visited edges: ", total_visited_edges);
  delete[] branch_id_for_each_edge;
  return out;
}

void CSRBasedPlanarVertexCutPartitioner::Redistributing(
    GraphID expected_n_of_branches,
    std::list<std::list<Edge>>* sorted_list_of_branches) {
  LOG_INFO("Redistribution.");
  VertexID n_new_branches = MAX_VERTEX_ID;
  while (n_new_branches >= expected_n_of_branches * 2) {
    n_new_branches = ceil(sorted_list_of_branches->size() / 2.0);
    LOG_INFO("Existing number of branches: ", n_new_branches,
             " expected number of branches: ", expected_n_of_branches);
    auto iter_begin = sorted_list_of_branches->begin();
    auto iter_end = sorted_list_of_branches->end();
    iter_end--;

    if (sorted_list_of_branches->size() % 2 == 0) {
      size_t count = 0;
      while (count < n_new_branches) {
        auto branch = sorted_list_of_branches->back();
        sorted_list_of_branches->pop_back();
        iter_begin->splice(iter_begin->end(), std::move(branch));
        iter_begin++;
        count++;
      }
    } else {
      size_t count = 0;
      while (count < n_new_branches - 1) {
        auto branch = sorted_list_of_branches->back();
        sorted_list_of_branches->pop_back();
        iter_begin->splice(iter_begin->end(), std::move(branch));
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
#ifdef TBB_FOUND
    std::for_each(std::execution::par, branch.begin(), branch.end(),
                  [&max_vid_per_edgelist, br_i](auto& e) {
                    WriteMax(&max_vid_per_edgelist[br_i], e.src);
                    WriteMax(&max_vid_per_edgelist[br_i], e.dst);
                  });
#else
    std::for_each(branch.begin(), branch.end(),
                  [&max_vid_per_edgelist, br_i](auto& e) {
                    WriteMax(&max_vid_per_edgelist[br_i], e.src);
                    WriteMax(&max_vid_per_edgelist[br_i], e.dst);
                  });
#endif
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
    Edges edges({(uint32_t)(visited[i].Count()), n_edges_per_edgelist[i],
                 max_vid_per_edgelist[i]});
    vec_edges.emplace_back(std::move(edges));
  }
  delete[] max_vid_per_edgelist;
  delete[] n_edges_per_edgelist;
  br_i = 0;
  for (const auto& branch : list_of_branches) {
    size_t index = 0;
    auto buffer_edges = vec_edges[br_i].get_base_ptr();
#ifdef TBB_FOUND
    std::for_each(std::execution::par, branch.begin(), branch.end(),
                  [&index, &buffer_edges, &br_i](auto& e) {
                    auto local_index = __sync_fetch_and_add(&index, 1);
                    buffer_edges[index] = e;
                  });
#else
    std::for_each(branch.begin(), branch.end(),
                  [&index, &buffer_edges, &br_i](auto& e) {
                    auto local_index = __sync_fetch_and_add(&index, 1);
                    buffer_edges[index] = e;
                  });
#endif
    br_i++;
  }

  return vec_edges;
}

}  // namespace sics::graph::tools::partitioner