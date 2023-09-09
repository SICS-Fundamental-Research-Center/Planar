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
using sics::graph::tools::common::Edge;
using sics::graph::tools::common::EdgelistMetadata;
using sics::graph::tools::common::Edges;
using sics::graph::tools::common::kUnconstrained;
using sics::graph::tools::common::StoreStrategy;

void Edgelist2CSR(const Edges& edges,
                  StoreStrategy store_strategy,
                  ImmutableCSRGraph* csr_graph) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(1);
  auto task_package = TaskPackage();
  task_package.reserve(parallelism);

  auto aligned_max_vid = ((edges.get_metadata().max_vid >> 6) << 6) + 64;
  auto visited = Bitmap(aligned_max_vid);
  auto num_inedges_by_vid = new VertexID[aligned_max_vid]();
  auto num_outedges_by_vid = new VertexID[aligned_max_vid]();
  VertexID min_vid = MAX_VERTEX_ID;

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (VertexID j = i; j < edges.get_metadata().num_edges;
           j += parallelism) {
        auto e = edges.get_edge_by_index(j);
        visited.SetBit(e.src);
        visited.SetBit(e.dst);
        WriteAdd(num_inedges_by_vid + e.dst, (VertexID)1);
        WriteAdd(num_outedges_by_vid + e.src, (VertexID)1);
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

  // Initialize the buffer_csr_vertices
  VertexID count_in_edges = 0, count_out_edges = 0;

  // Malloc space for each vertex.
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
        WriteAdd(&count_in_edges, buffer_csr_vertices[j].indegree);
        WriteAdd(&count_out_edges, buffer_csr_vertices[j].outdegree);
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete[] num_inedges_by_vid;
  delete[] num_outedges_by_vid;

  VertexID* offset_in_edges = new VertexID[aligned_max_vid]();
  VertexID* offset_out_edges = new VertexID[aligned_max_vid]();
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (VertexID j = i; j < edges.get_metadata().num_edges;
           j += parallelism) {
        auto e = edges.get_edge_by_index(j);
        auto offset_out = __sync_fetch_and_add(offset_out_edges + e.src, 1);
        auto offset_in = __sync_fetch_and_add(offset_in_edges + e.dst, 1);
        buffer_csr_vertices[e.src].outgoing_edges[offset_out] = e.dst;
        buffer_csr_vertices[e.dst].incoming_edges[offset_in] = e.src;
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  // Construct CSR graph.
  auto buffer_globalid = new VertexID[edges.get_metadata().num_vertices]();
  auto buffer_indegree = new VertexID[edges.get_metadata().num_vertices]();
  auto buffer_outdegree = new VertexID[edges.get_metadata().num_vertices]();

  VertexID vid = 0;
  for (VertexID i = 0; i < aligned_max_vid; i++) {
    if (!visited.GetBit(i)) continue;
    buffer_globalid[vid] = buffer_csr_vertices[i].vid;
    buffer_indegree[vid] = buffer_csr_vertices[i].indegree;
    buffer_outdegree[vid] = buffer_csr_vertices[i].outdegree;
    vid++;
  }

  auto buffer_in_offset = new VertexID[edges.get_metadata().num_vertices]();
  auto buffer_out_offset = new VertexID[edges.get_metadata().num_vertices]();

  // Compute offset for each vertex.
  for (VertexID i = 1; i < edges.get_metadata().num_vertices; i++) {
    buffer_in_offset[i] = buffer_in_offset[i - 1] + buffer_indegree[i - 1];
    buffer_out_offset[i] = buffer_out_offset[i - 1] + buffer_outdegree[i - 1];
  }

  auto buffer_in_edges = new VertexID[count_in_edges]();
  auto buffer_out_edges = new VertexID[count_out_edges]();

  // Fill edges.
  vid = 0;
  for (VertexID i = 0; i < aligned_max_vid; i++) {
    if (!visited.GetBit(i)) continue;
    if (buffer_csr_vertices[i].indegree != 0) {
      memcpy(buffer_in_edges + buffer_in_offset[vid],
             buffer_csr_vertices[i].incoming_edges,
             buffer_csr_vertices[i].indegree * sizeof(VertexID));
      delete[] buffer_csr_vertices[i].incoming_edges;
    }
    if (buffer_csr_vertices[i].outdegree != 0) {
      memcpy(buffer_out_edges + buffer_out_offset[vid],
             buffer_csr_vertices[i].outgoing_edges,
             buffer_csr_vertices[i].outdegree * sizeof(VertexID));
      delete[] buffer_csr_vertices[i].outgoing_edges;
    }
    vid++;
  }
  delete[] buffer_csr_vertices;

  // Sort edges.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (VertexID j = i; j < edges.get_metadata().num_vertices;
           j += parallelism) {
        if (buffer_indegree[j] != 0) {
          std::sort(buffer_in_edges + buffer_in_offset[j],
                    buffer_in_edges + buffer_in_offset[j] + buffer_indegree[j]);
        }
        if (buffer_outdegree[j] != 0) {
          std::sort(
              buffer_out_edges + buffer_out_offset[j],
              buffer_out_edges + buffer_out_offset[j] + buffer_outdegree[j]);
        }
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

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