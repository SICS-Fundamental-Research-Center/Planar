#include "format_converter.h"

namespace sics::graph::tools::util {
namespace format_converter {

using sics::graph::core::common::Bitmap;
using sics::graph::core::common::EdgeIndex;
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
using sics::graph::tools::common::Edge;
using sics::graph::tools::common::EdgelistMetadata;
using sics::graph::tools::common::Edges;
using sics::graph::tools::common::kUnconstrained;
using sics::graph::tools::common::StoreStrategy;

void Edgelist2CSR(const Edges& edges, StoreStrategy store_strategy,
                  ImmutableCSRGraph* csr_graph) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();
  task_package.reserve(parallelism);

  auto aligned_max_vid = (((edges.get_metadata().max_vid + 1) >> 6) << 6) + 64;
  auto visited = Bitmap(aligned_max_vid);
  auto num_inedges_by_vid = new VertexID[aligned_max_vid]();
  auto num_outedges_by_vid = new VertexID[aligned_max_vid]();
  VertexID min_vid = MAX_VERTEX_ID;

  LOG_INFO("Prepare for converting edgelist to CSR graph.");
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (EdgeIndex j = i; j < edges.get_metadata().num_edges;
           j += parallelism) {
        auto e = edges.get_edge_by_index(j);
        visited.SetBit(e.src);
        visited.SetBit(e.dst);
        WriteAdd(num_inedges_by_vid + e.dst, (VertexID) 1);
        WriteAdd(num_outedges_by_vid + e.src, (VertexID) 1);
        WriteMin(&min_vid, e.src);
        WriteMin(&min_vid, e.dst);
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  // Initialize the buffer_csr_vertices
  auto buffer_csr_vertices = new Vertex[aligned_max_vid]();
  EdgeIndex count_in_edges = 0, count_out_edges = 0;

  LOG_INFO("Malloc space for each vertex.");
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (VertexID j = i; j < aligned_max_vid; j += parallelism) {
        if (!visited.GetBit(j)) continue;
        buffer_csr_vertices[j].vid = j;
        buffer_csr_vertices[j].indegree = num_inedges_by_vid[j];
        buffer_csr_vertices[j].outdegree = num_outedges_by_vid[j];
        buffer_csr_vertices[j].incoming_edges =
            new VertexID[num_inedges_by_vid[j]]();
        buffer_csr_vertices[j].outgoing_edges =
            new VertexID[num_outedges_by_vid[j]]();
        WriteAdd(&count_in_edges, (EdgeIndex)buffer_csr_vertices[j].indegree);
        WriteAdd(&count_out_edges, (EdgeIndex)buffer_csr_vertices[j].outdegree);
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete[] num_inedges_by_vid;
  delete[] num_outedges_by_vid;

  EdgeIndex* offset_in_edges = new EdgeIndex[aligned_max_vid]();
  EdgeIndex* offset_out_edges = new EdgeIndex[aligned_max_vid]();
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (EdgeIndex j = i; j < edges.get_metadata().num_edges;
           j += parallelism) {
        auto e = edges.get_edge_by_index(j);
        EdgeIndex offset_out = 0, offset_in = 0;
        offset_out = __sync_fetch_and_add(offset_out_edges + e.src, 1);
        offset_in = __sync_fetch_and_add(offset_in_edges + e.dst, 1);
        buffer_csr_vertices[e.src].outgoing_edges[offset_out] = e.dst;
        buffer_csr_vertices[e.dst].incoming_edges[offset_in] = e.src;
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete[] offset_in_edges;
  delete[] offset_out_edges;

  // Construct CSR graph.
  auto buffer_globalid = new VertexID[edges.get_metadata().num_vertices]();
  auto buffer_indegree = new VertexID[edges.get_metadata().num_vertices]();
  auto buffer_outdegree = new VertexID[edges.get_metadata().num_vertices]();

  VertexID vid = 0;
  auto vid_map = new VertexID[aligned_max_vid]();
  for (VertexID i = 0; i < aligned_max_vid; i++) {
    if (!visited.GetBit(i)) continue;
    buffer_globalid[vid] = buffer_csr_vertices[i].vid;
    buffer_indegree[vid] = buffer_csr_vertices[i].indegree;
    buffer_outdegree[vid] = buffer_csr_vertices[i].outdegree;
    vid_map[i] = vid;
    vid++;
  }
  auto buffer_in_offset = new EdgeIndex[edges.get_metadata().num_vertices]();
  auto buffer_out_offset = new EdgeIndex[edges.get_metadata().num_vertices]();

  LOG_INFO("Compute offset for each vertex.");
  for (VertexID i = 1; i < edges.get_metadata().num_vertices; i++) {
    buffer_in_offset[i] = buffer_in_offset[i - 1] + buffer_indegree[i - 1];
    buffer_out_offset[i] = buffer_out_offset[i - 1] + buffer_outdegree[i - 1];
  }

  auto buffer_in_edges = new VertexID[count_in_edges]();
  auto buffer_out_edges = new VertexID[count_out_edges]();

  // Fill edges.
  LOG_INFO("Fill buffer_edges and sort out.");
  vid = 0;
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (VertexID j = i; j < aligned_max_vid; j += parallelism) {
        if (!visited.GetBit(j)) continue;
        if (buffer_csr_vertices[j].indegree != 0) {
          memcpy(buffer_in_edges + buffer_in_offset[vid_map[j]],
                 buffer_csr_vertices[j].incoming_edges,
                 buffer_csr_vertices[j].indegree * sizeof(VertexID));
          std::sort(buffer_in_edges + buffer_in_offset[vid_map[j]],
                    buffer_in_edges + buffer_in_offset[vid_map[j]] +
                        buffer_indegree[vid_map[j]]);
        }
        if (buffer_csr_vertices[j].outdegree != 0) {
          memcpy(buffer_out_edges + buffer_out_offset[vid_map[j]],
                 buffer_csr_vertices[j].outgoing_edges,
                 buffer_csr_vertices[j].outdegree * sizeof(VertexID));
          std::sort(buffer_out_edges + buffer_out_offset[vid_map[j]],
                    buffer_out_edges + buffer_out_offset[vid_map[j]] +
                        buffer_outdegree[vid_map[j]]);
        }
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  delete[] buffer_csr_vertices;
  delete[] vid_map;

  csr_graph->SetGlobalIDBuffer(buffer_globalid);
  csr_graph->SetInDegreeBuffer(buffer_indegree);
  csr_graph->SetOutDegreeBuffer(buffer_outdegree);
  csr_graph->SetInOffsetBuffer(buffer_in_offset);
  csr_graph->SetOutOffsetBuffer(buffer_out_offset);
  csr_graph->SetIncomingEdgesBuffer(buffer_in_edges);
  csr_graph->SetOutgoingEdgesBuffer(buffer_out_edges);
  csr_graph->set_num_vertices(edges.get_metadata().num_vertices);
  csr_graph->set_num_incoming_edges(count_in_edges);
  csr_graph->set_num_outgoing_edges(count_out_edges);
  csr_graph->set_max_vid(edges.get_metadata().max_vid);
  csr_graph->set_min_vid(min_vid);
}

}  // namespace format_converter
}  // namespace sics::graph::tools::util