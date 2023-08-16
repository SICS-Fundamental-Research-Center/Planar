#include "format_converter.h"

namespace sics::graph::tools::util {
namespace format_converter {

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
using Vertex = sics::graph::core::data_structures::graph::ImmutableCSRVertex;
using sics::graph::core::data_structures::graph::ImmutableCSRGraph;
using sics::graph::tools::common::EdgelistMetadata;
using sics::graph::tools::common::kUnconstrained;
using sics::graph::tools::common::StoreStrategy;

void Edgelist2CSR(VertexID* buffer_edges,
                  const EdgelistMetadata& edgelist_metadata,
                  const StoreStrategy store_strategy,
                  ImmutableCSRGraph* csr_graph) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(1);
  auto task_package = TaskPackage();

  auto aligned_max_vid = ((edgelist_metadata.max_vid >> 6) << 6) + 64;
  auto visited = Bitmap(aligned_max_vid);
  visited.Clear();
  auto num_inedges_by_vid =
      (VertexID*)malloc(sizeof(VertexID) * aligned_max_vid);
  auto num_outedges_by_vid =
      (VertexID*)malloc(sizeof(VertexID) * aligned_max_vid);
  memset(num_inedges_by_vid, 0, sizeof(VertexID) * aligned_max_vid);
  memset(num_outedges_by_vid, 0, sizeof(VertexID) * aligned_max_vid);
  VertexID max_vid = 0, min_vid = MAX_VERTEX_ID;

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &edgelist_metadata, &buffer_edges,
                           &num_inedges_by_vid, &num_outedges_by_vid, &max_vid,
                           &min_vid, &visited]() {
      for (VertexID j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto src = buffer_edges[j * 2];
        auto dst = buffer_edges[j * 2 + 1];
        visited.SetBit(src);
        visited.SetBit(dst);
        WriteAdd(num_inedges_by_vid + dst, (VertexID)1);
        WriteAdd(num_outedges_by_vid + src, (VertexID)1);
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

  // Initialize the buffer_csr_vertices
  Vertex* buffer_csr_vertices =
      (Vertex*)malloc(sizeof(Vertex) * aligned_max_vid);
  memset((char*)buffer_csr_vertices, 0, sizeof(Vertex) * aligned_max_vid);

  // Initialize the buffer_csr_vertices
  VertexID count_in_edges = 0, count_out_edges = 0;

  // Malloc space for each vertex.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &aligned_max_vid, &buffer_edges,
                           &num_inedges_by_vid, &num_outedges_by_vid,
                           &buffer_csr_vertices, &count_in_edges,
                           &count_out_edges, &visited]() {
      for (VertexID j = i; j < aligned_max_vid; j += parallelism) {
        if (!visited.GetBit(j)) continue;
        auto u = Vertex();
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

  VertexID* offset_in_edges =
      (VertexID*)malloc(sizeof(VertexID) * aligned_max_vid);
  VertexID* offset_out_edges =
      (VertexID*)malloc(sizeof(VertexID) * aligned_max_vid);
  memset(offset_in_edges, 0, sizeof(VertexID) * aligned_max_vid);
  memset(offset_out_edges, 0, sizeof(VertexID) * aligned_max_vid);
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &edgelist_metadata, &buffer_edges,
                           &offset_in_edges, &offset_out_edges,
                           &buffer_csr_vertices]() {
      for (VertexID j = i; j < edgelist_metadata.num_edges; j += parallelism) {
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
      (VertexID*)malloc(sizeof(VertexID) * edgelist_metadata.num_vertices);
  auto buffer_outdegree =
      (VertexID*)malloc(sizeof(VertexID) * edgelist_metadata.num_vertices);

  VertexID vid = 0;
  parallelism = 1;
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &vid, &visited, &aligned_max_vid,
                           &buffer_globalid, &buffer_indegree,
                           &buffer_outdegree, &buffer_csr_vertices]() {
      for (VertexID j = i; j < aligned_max_vid; j += parallelism) {
        if (!visited.GetBit(j)) continue;
        auto local_vid = __sync_fetch_and_add(&vid, 1);
        buffer_globalid[local_vid] = buffer_csr_vertices[j].vid;
        buffer_indegree[local_vid] = buffer_csr_vertices[j].indegree;
        buffer_outdegree[local_vid] = buffer_csr_vertices[j].outdegree;
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  auto buffer_in_offset =
      (VertexID*)malloc(sizeof(VertexID) * edgelist_metadata.num_vertices);
  auto buffer_out_offset =
      (VertexID*)malloc(sizeof(VertexID) * edgelist_metadata.num_vertices);
  memset(buffer_in_offset, 0,
         sizeof(VertexID) * edgelist_metadata.num_vertices);
  memset(buffer_out_offset, 0,
         sizeof(VertexID) * edgelist_metadata.num_vertices);

  // Compute offset for each vertex.
  for (VertexID i = 1; i < edgelist_metadata.num_vertices; i++) {
    buffer_in_offset[i] = buffer_in_offset[i - 1] + buffer_indegree[i - 1];
    buffer_out_offset[i] = buffer_out_offset[i - 1] + buffer_outdegree[i - 1];
  }

  auto buffer_in_edges = (VertexID*)malloc(sizeof(VertexID) * count_in_edges);
  auto buffer_out_edges = (VertexID*)malloc(sizeof(VertexID) * count_out_edges);
  memset(buffer_out_edges, 0, sizeof(VertexID) * count_out_edges);
  memset(buffer_in_edges, 0, sizeof(VertexID) * count_in_edges);

  // Copy edges into buffer.;
  vid = 0;
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &vid, &visited, &aligned_max_vid,
                           &buffer_in_edges, &buffer_out_edges,
                           &buffer_in_offset, &buffer_out_offset,
                           &buffer_csr_vertices, &count_out_edges]() {
      for (VertexID j = i; j < aligned_max_vid; j += parallelism) {
        if (!visited.GetBit(j)) continue;
        auto local_vid = __sync_fetch_and_add(&vid, 1);
        if (buffer_csr_vertices[j].indegree != 0)
          memcpy(buffer_in_edges + buffer_in_offset[local_vid],
                 buffer_csr_vertices[j].in_edges,
                 buffer_csr_vertices[j].indegree * sizeof(VertexID));
        if (buffer_csr_vertices[j].outdegree != 0)
          memcpy(buffer_out_edges + buffer_out_offset[local_vid],
                 buffer_csr_vertices[j].out_edges,
                 buffer_csr_vertices[j].outdegree * sizeof(VertexID));
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  csr_graph->SetGlobalID(buffer_globalid);
  csr_graph->SetInDegree(buffer_indegree);
  csr_graph->SetOutDegree(buffer_outdegree);
  csr_graph->SetInOffset(buffer_in_offset);
  csr_graph->SetOutOffset(buffer_out_offset);
  csr_graph->SetInEdges(buffer_in_edges);
  csr_graph->SetOutEdges(buffer_out_edges);
  csr_graph->set_num_vertices(edgelist_metadata.num_vertices);
  csr_graph->set_num_incoming_edges(count_in_edges);
  csr_graph->set_num_outgoing_edges(count_out_edges);
  csr_graph->set_max_vid(max_vid);
  csr_graph->set_min_vid(min_vid);
}

}  // namespace format_converter
}  // namespace sics::graph::tools::util