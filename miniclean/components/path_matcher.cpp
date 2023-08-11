#include "miniclean/components/path_matcher.h"

#include <iostream>
#include <memory>
#include <string>

#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/io/basic_reader.h"

namespace sics::graph::miniclean::components {

using BasicReader = sics::graph::core::io::BasicReader;
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using ThreadPool = sics::graph::core::common::ThreadPool;

void PathMatcher::LoadGraph(const std::string& data_path) {
  // Prepare reader.
  BasicReader reader;

  // Read subgraph.
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_graph =
      std::make_unique<SerializedImmutableCSRGraph>();
  reader.ReadSubgraph(data_path, serialized_graph.get(), 1);

  // Deserialize subgraph.
  ThreadPool thread_pool(1);
  miniclean_csr_graph_.Deserialize(thread_pool, std::move(serialized_graph));
}

void PathMatcher::BuildCandidateSet(VertexLabel num_label) {
  VertexLabel* vertex_label = miniclean_csr_graph_.get_vertex_label();
  for (VertexID i = 0; i < miniclean_csr_graph_config_.num_vertex * 2; i += 2) {
    for (VertexLabel j = 0; j < num_label; j++) {
      if (vertex_label[i + 1] == j + 1) candidates_[j].insert(vertex_label[i]);
    }
  }
}

void PathMatcher::PathMatching() {
  // initialize the result vector
  matched_results_.resize(path_patterns_.size());
  for (size_t i=0; i<path_patterns_.size(); i++) {
    PathMatching(path_patterns_[i], matched_results_[i]);
  }
}

void PathMatcher::PathMatching(const std::vector<VertexLabel>& path_pattern,
                               std::vector<std::vector<VertexID>>& results) {
  // Prepare candidates.
  VertexLabel start_label = path_pattern[0];
  std::set<VertexID> candidates = candidates_[start_label - 1];
  std::vector<VertexID> partial_result;
  path_match_recur(path_pattern, 0, candidates, partial_result, results);
}

void PathMatcher::path_match_recur(
    const std::vector<VertexLabel>& path_pattern, size_t match_position,
    std::set<VertexID>& candidates, std::vector<VertexID>& partial_result,
    std::vector<std::vector<VertexID>>& results) {
  // Return condition.
  if (match_position == path_pattern.size()) {
    results.push_back(partial_result);
    return;
  }

  for (VertexID candidate : candidates) {
    // Scan the out-edges of the candidate.
    size_t cand_out_degree = miniclean_csr_graph_.GetOutDegree()[candidate];
    size_t cand_out_offset = miniclean_csr_graph_.GetOutOffset()[candidate];
    std::set<VertexID> next_candidates;
    for (size_t i = 0; i < cand_out_degree; i++) {
      VertexID out_edge_id =
          miniclean_csr_graph_.GetOutEdges()[cand_out_offset + i];
      VertexLabel out_edge_label =
          miniclean_csr_graph_.get_vertex_label()[out_edge_id * 2 + 1];
      bool continue_flag = false;
      // Check whether cycle exists.
      for (VertexID vid : partial_result) {
        if (vid == out_edge_id) {
          continue_flag = true;
          break;
        }
      }
      if (continue_flag) continue;
      // Check whether the label matches.
      if (out_edge_label == path_pattern[match_position+1]) {
        next_candidates.insert(out_edge_id);
      }
    }
    partial_result.push_back(candidate);
    path_match_recur(path_pattern, match_position + 1, next_candidates,
                     partial_result, results);
    partial_result.pop_back();
  }
}

void PathMatcher::PrintMatchedResults() {
  for (size_t i = 0; i < matched_results_.size(); i++) {
    std::cout << "Pattern " << i << ": ";
    for (size_t j = 0; j < path_patterns_[i].size(); j++) {
      std::cout << path_patterns_[i][j] << " ";
    }
    std::cout << "\n";

    for (size_t j = 0; j < matched_results_[i].size(); j++) {
      std::cout << "Match " << j << ": ";
      for (size_t k = 0; k < matched_results_[i][j].size(); k++) {
        std::cout << matched_results_[i][j][k] << " ";
      }
      std::cout << "\n";
    }

    std::cout << "-------------------------\n";
  }
}
}  // namespace sics::graph::miniclean::components
