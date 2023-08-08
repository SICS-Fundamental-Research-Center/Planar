#include <iostream>
#include <memory>

#include "miniclean/graphs/miniclean_csr_graph.h"
#include "miniclean/graphs/miniclean_csr_graph_config.h"
#include "core/io/basic_reader.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/common/multithreading/thread_pool.h"

using MinicleanCSRGraph =  sics::graph::miniclean::graphs::MinicleanCSRGraph;
using MiniGraphCSRGraphConfig = sics::graph::miniclean::graphs::MinicleanCSRGraphConfig;
using SerializedImmutableCSRGraph = sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using BasicReader = sics::graph::core::io::BasicReader;
using VertexID = sics::graph::core::common::VertexID;
using ThreadPool = sics::graph::core::common::ThreadPool;


size_t compute_total_size(MiniGraphCSRGraphConfig config) {
  VertexID aligned_max_vertex = (config.max_vertex + 63) / 64 * 64;

  size_t size_localid = sizeof(VertexID) * config.num_vertex;
  size_t size_globalid = sizeof(VertexID) * config.num_vertex;
  size_t size_indegree = sizeof(size_t) * config.num_vertex;
  size_t size_outdegree = sizeof(size_t) * config.num_vertex;
  size_t size_in_offset = sizeof(size_t) * config.num_vertex;
  size_t size_out_offset = sizeof(size_t) * config.num_vertex;
  size_t size_in_edges = sizeof(VertexID) * config.sum_in_edges;
  size_t size_out_edges = sizeof(VertexID) * config.sum_out_edges;
  size_t size_localid_by_globalid = sizeof(VertexID) * aligned_max_vertex;

  size_t expected_size = size_localid + size_globalid + size_indegree +
                          size_outdegree + size_in_offset + size_out_offset +
                          size_in_edges + size_out_edges +
                          size_localid_by_globalid;

  return expected_size;
}


int main() {
  BasicReader reader;
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_graph = std::make_unique<SerializedImmutableCSRGraph>();
  MiniGraphCSRGraphConfig config = {
      9302681,   // num_vertex
      9302680,   // max_vertex
      73286905,  // sum_in_degree
      73286905,  // sum_out_degree
  };
  MinicleanCSRGraph graph(0, std::move(config));

  size_t expected_size = compute_total_size(config);

  float expect_size_gb = float(expected_size) / 1024.0 / 1024.0 / 1024.0;

  std::cout << "expected size: " << expected_size << std::endl;
  std::cout << "expected size: " << expect_size_gb << " GB" << std::endl;

  reader.ReadSubgraph(
      "/home/baiwc/workspace/graph-systems/data/dblp_test/0",
      serialized_graph.get(), 1);

  size_t loaded_size = serialized_graph->GetCSRBuffer().front().front().GetSize();
  std::cout << "loaded size: " << loaded_size << std::endl;
  float loaded_size_gb = float(loaded_size) / 1024.0 / 1024.0 / 1024.0;
  std::cout << "loaded size: " << loaded_size_gb << " GB" << std::endl;

  ThreadPool thread_pool(1);
  graph.Deserialize(thread_pool, std::move(serialized_graph));

  

  return 0;
}