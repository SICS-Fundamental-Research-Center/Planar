#include "adapter.h"

using sics::graph::core::common::Bitmap;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexLabel;
using sics::graph::core::data_structures::GraphMetadata;
using sics::graph::core::data_structures::SubgraphMetadata;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::atomic::WriteMax;
using sics::graph::core::util::atomic::WriteMin;
using std::filesystem::create_directory;
using std::filesystem::exists;
using namespace sics::graph::tools;

namespace sics::graph::tools {

void GraphAdapter::Edgelist2CSR(
    VertexID* buffer_edges, EdgelistMetadata edgelist_metadata,
    ImmutableCSRGraph& csr_graph,
    const StoreStrategy store_strategy = kUnconstrained) {

  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();

  auto aligned_max_vid = ((edgelist_metadata.max_vid >> 6) << 6) + 64;
  auto visited = Bitmap(aligned_max_vid);
  visited.Clear();
  auto num_inedges_by_vid = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  auto num_outedges_by_vid = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  memset(num_inedges_by_vid, 0, sizeof(size_t) * aligned_max_vid);
  memset(num_outedges_by_vid, 0, sizeof(size_t) * aligned_max_vid);
  VertexID max_vid = 0, min_vid = MAX_VERTEX_ID;

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &edgelist_metadata, &buffer_edges,
                           &num_inedges_by_vid, &num_outedges_by_vid, &max_vid,
                           &min_vid, &visited]() {
      for (size_t j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto src = buffer_edges[j * 2];
        auto dst = buffer_edges[j * 2 + 1];
        visited.SetBit(src);
        visited.SetBit(dst);
        WriteAdd(num_inedges_by_vid + dst, (size_t)1);
        WriteAdd(num_outedges_by_vid + src, (size_t)1);
        WriteMin(&min_vid, src);
        WriteMin(&min_vid, dst);
        WriteMax(&max_vid, src);
        WriteMax(&max_vid, dst);
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  TMPCSRVertex* buffer_csr_vertices =
      (TMPCSRVertex*)malloc(sizeof(TMPCSRVertex) * aligned_max_vid);
  memset((char*)buffer_csr_vertices, 0, sizeof(TMPCSRVertex) * aligned_max_vid);

  // Initialize the buffer_csr_vertices
  size_t count_in_edges = 0, count_out_edges = 0;

  // Malloc space for each vertex.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &aligned_max_vid, &buffer_edges,
                           &num_inedges_by_vid, &num_outedges_by_vid,
                           &buffer_csr_vertices, &count_in_edges,
                           &count_out_edges, &visited]() {
      for (size_t j = i; j < aligned_max_vid; j += parallelism) {
        if (!visited.GetBit(j)) continue;
        auto u = TMPCSRVertex();
        u.vid = j;
        u.indegree = num_inedges_by_vid[j];
        u.outdegree = num_outedges_by_vid[j];
        u.in_edges = (VertexID*)malloc(sizeof(VertexID) * u.indegree);
        u.out_edges = (VertexID*)malloc(sizeof(VertexID) * u.outdegree);
        WriteAdd(&count_in_edges, u.indegree);
        WriteAdd(&count_out_edges, u.outdegree);
        buffer_csr_vertices[j] = u;
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete num_inedges_by_vid;
  delete num_outedges_by_vid;

  size_t* offset_in_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  size_t* offset_out_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  memset(offset_in_edges, 0, sizeof(size_t) * aligned_max_vid);
  memset(offset_out_edges, 0, sizeof(size_t) * aligned_max_vid);
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &edgelist_metadata, &buffer_edges,
                           &offset_in_edges, &offset_out_edges,
                           &buffer_csr_vertices]() {
      for (size_t j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto src = buffer_edges[j * 2];
        auto dst = buffer_edges[j * 2 + 1];
        auto offset_out = __sync_fetch_and_add(offset_out_edges + src, 1);
        auto offset_in = __sync_fetch_and_add(offset_in_edges + dst, 1);
        buffer_csr_vertices[src].out_edges[offset_out] = dst;
        buffer_csr_vertices[dst].in_edges[offset_in] = src;
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  // Construct CSR graph.
  auto buffer_globalid =
      (VertexID*)malloc(sizeof(VertexID) * edgelist_metadata.num_vertices);
  auto buffer_indegree =
      (size_t*)malloc(sizeof(size_t) * edgelist_metadata.num_vertices);
  auto buffer_outdegree =
      (size_t*)malloc(sizeof(size_t) * edgelist_metadata.num_vertices);

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &edgelist_metadata, &buffer_globalid,
                           &buffer_indegree, &buffer_outdegree,
                           &buffer_csr_vertices]() {
      for (size_t j = i; j < edgelist_metadata.num_vertices; j += parallelism) {
        buffer_globalid[j] = buffer_csr_vertices[j].vid;
        buffer_indegree[j] = buffer_csr_vertices[j].indegree;
        buffer_outdegree[j] = buffer_csr_vertices[j].outdegree;
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  auto buffer_in_offset =
      (size_t*)malloc(sizeof(size_t) * edgelist_metadata.num_vertices);
  auto buffer_out_offset =
      (size_t*)malloc(sizeof(size_t) * edgelist_metadata.num_vertices);
  memset(buffer_in_offset, 0, sizeof(size_t) * edgelist_metadata.num_vertices);
  memset(buffer_out_offset, 0, sizeof(size_t) * edgelist_metadata.num_vertices);

  // Compute offset for each vertex.
  for (size_t i = 1; i < edgelist_metadata.num_vertices; i++) {
    buffer_in_offset[i] = buffer_in_offset[i - 1] + buffer_indegree[i - 1];
    buffer_out_offset[i] = buffer_out_offset[i - 1] + buffer_outdegree[i - 1];
  }

  auto buffer_in_edges = (VertexID*)malloc(sizeof(VertexID) * count_in_edges);
  auto buffer_out_edges = (VertexID*)malloc(sizeof(VertexID) * count_out_edges);
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &edgelist_metadata, &buffer_in_edges,
                           &buffer_out_edges, &buffer_in_offset,
                           &buffer_out_offset, &buffer_csr_vertices]() {
      for (size_t j = i; j < edgelist_metadata.num_vertices; j += parallelism) {
        memcpy(buffer_in_edges + buffer_in_offset[j],
               buffer_csr_vertices[j].in_edges,
               buffer_csr_vertices[j].indegree * sizeof(VertexID));
        memcpy(buffer_out_edges + buffer_out_offset[j],
               buffer_csr_vertices[j].out_edges,
               buffer_csr_vertices[j].outdegree * sizeof(VertexID));
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  csr_graph.SetGlobalIDbyIndex(buffer_globalid);
  csr_graph.SetInDegree(buffer_indegree);
  csr_graph.SetOutDegree(buffer_outdegree);
  csr_graph.SetInOffset(buffer_in_offset);
  csr_graph.SetOutOffset(buffer_out_offset);
  csr_graph.SetInEdges(buffer_in_edges);
  csr_graph.SetOutEdges(buffer_out_edges);
  csr_graph.set_num_vertices(edgelist_metadata.num_vertices);
  csr_graph.set_num_incoming_edges(count_in_edges);
  csr_graph.set_num_outgoing_edges(count_out_edges);
  csr_graph.set_max_vid(max_vid);
  csr_graph.set_min_vid(min_vid);
}

}  // namespace sics::graph::tools