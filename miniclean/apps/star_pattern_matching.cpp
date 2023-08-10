#include <iostream>
#include <memory>
#include <vector>

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

void path_matching(
    const MinicleanCSRGraph& graph, const std::vector<VertexID>& path_pattern,
    int position, std::set<VertexID> candidates,
    std::vector<std::pair<VertexID, VertexID>>& partial_result,
    std::vector<std::vector<std::pair<VertexID, VertexID>>>* matched_results) {

  if (position == path_pattern.size()) {
    // Print the matched pattern.
    std::cout << "Matched pattern: ";
    for (auto& v : path_pattern) {
      std::cout << v << " ";
    }
    std::cout << std::endl;
    // Print the matched result.
    std::cout << "Matched result: ";
    for (auto& pair : partial_result) {
      std::cout << "(" << pair.first << ", " << pair.second << ") ";
    }
    std::cout << std::endl;
    std::cout << "--------------------------" << std::endl;
    matched_results->push_back(partial_result);
    return;
  }
  
  VertexID to_be_matched = path_pattern[position];
  // Iterate the candidates
  for (VertexID candidate : candidates) {
    // Whether candidate has an out edge connects to `to_be_matched`?
    size_t cand_out_degree = graph.GetOutDegree()[candidate];
    size_t cand_out_offset = graph.GetOutOffset()[candidate];
    std::set<VertexID> tmp_candidate;
    // scan the out edges of candidate.
    for (size_t i = 0; i < cand_out_degree; i++) {
      VertexID out_edge_id = graph.GetOutEdges()[cand_out_offset + i];
      VertexID out_edge_label = graph.get_vertex_label()[out_edge_id * 2 + 1]; 
      if (partial_result.size() > 0 && partial_result[0].first == out_edge_id) continue;
      bool continue_flag = false;
      for (auto v : partial_result) {
        if (v.second == out_edge_id) {
          continue_flag = true;
          break;
        }
      }
      if (continue_flag) continue;
      if (out_edge_label == to_be_matched) {
        // Add the candidate to the tmp_candidate set.
        tmp_candidate.insert(out_edge_id);
      }
    }
    if (tmp_candidate.size() > 0) {
      for (auto tmp_c : tmp_candidate) {
        partial_result.push_back(std::make_pair(candidate, tmp_c));
        path_matching(graph, path_pattern, position + 1, {tmp_c}, partial_result, matched_results);
        partial_result.pop_back();
      }
    }
  }
}

int main() {
  BasicReader reader;
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_graph = std::make_unique<SerializedImmutableCSRGraph>();
  // MiniGraphCSRGraphConfig config = {
  //     12009216,   // num_vertex
  //     12009215,   // max_vertex
  //     73286905,   // sum_in_degree
  //     73286905,   // sum_out_degree
  // };
  MiniGraphCSRGraphConfig config = {
      56,   // num_vertex
      55,   // max_vertex
      100,   // sum_in_degree
      100,   // sum_out_degree
  };
  MinicleanCSRGraph graph(0, std::move(config));

  size_t expected_size = compute_total_size(config);

  float expect_size_gb = float(expected_size) / 1024.0 / 1024.0 / 1024.0;

  std::cout << "expected size: " << expected_size << std::endl;
  std::cout << "expected size: " << expect_size_gb << " GB" << std::endl;

  // reader.ReadSubgraph(
  //     "/home/baiwc/workspace/graph-systems/data/dblp_test/0",
  //     serialized_graph.get(), 1);
  reader.ReadSubgraph(
      "/home/baiwc/workspace/graph-systems/data/small_graph/0",
      serialized_graph.get(), 1);

  size_t loaded_size = serialized_graph->GetCSRBuffer().front().front().GetSize();
  std::cout << "loaded size: " << loaded_size << std::endl;
  float loaded_size_gb = float(loaded_size) / 1024.0 / 1024.0 / 1024.0;
  std::cout << "loaded size: " << loaded_size_gb << " GB" << std::endl;

  ThreadPool thread_pool(1);
  graph.Deserialize(thread_pool, std::move(serialized_graph));

  std::cout << "end of deserialize." << std::endl;

  // Construct candidate set.
  std::set<VertexID> candidates[4];
  VertexID* vertex_label = graph.get_vertex_label();
  for (VertexID i = 0; i < config.num_vertex * 2; i += 2) {
    if (vertex_label[i + 1] == 1) candidates[0].insert(vertex_label[i]);
    if (vertex_label[i + 1] == 2) candidates[1].insert(vertex_label[i]);
    if (vertex_label[i + 1] == 3) candidates[2].insert(vertex_label[i]);
    if (vertex_label[i + 1] == 4) candidates[3].insert(vertex_label[i]);
  }

  std::cout << "expected candidate size: " << config.num_vertex << std::endl;
  std::cout << "candidate size: "
            << candidates[0].size() + candidates[1].size() + candidates[2].size() +
                   candidates[3].size()
            << std::endl;

  // Path patterns to be matched.
  // std::vector<std::vector<VertexID>> path_patterns = {
  //     {1, 1},    {1, 2},    {1, 4},    {4, 3},      {1, 1, 1},
  //     {1, 1, 2}, {1, 1, 4}, {1, 4, 3}, {1, 1, 4, 3}};
  std::vector<std::vector<VertexID>> path_patterns = {
      {1, 1}, {1, 2}, {1, 3}, {2, 1},    {2, 2},    {2, 3},
      {3, 1}, {3, 2}, {3, 3}, {1, 1, 1}, {1, 1, 2}, {1, 1, 3}, {1, 1, 1, 1},
  };
  // std::vector<std::vector<VertexID>> path_patterns = {{2, 2}};

  // Match patterns globally.
  std::vector<std::vector<std::pair<VertexID, VertexID>>> matched_results;
  std::vector<std::pair<VertexID, VertexID>> temp_matched_results;

  for (std::vector<VertexID> pattern : path_patterns) {

    // Match a single pattern.

    std::cout << "pattern: ";
    for (VertexID v : pattern) {
      std::cout << v << " ";
    }
    std::cout << std::endl;
    
    path_matching(graph, pattern, 1, candidates[pattern[0] - 1], temp_matched_results, &matched_results); 
    temp_matched_results.clear();

    // Length of matched_results
    std::cout << "matched results size: " << matched_results.size() << std::endl;
  }

  return 0;
}