#ifndef MINICLEAN_COMPONENTS_PATH_MATCHER_H_
#define MINICLEAN_COMPONENTS_PATH_MATCHER_H_

#include <unordered_set>
#include <vector>

#include "core/common/types.h"
#include "miniclean/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::components {

class PathMatcher {
 private:
  using MiniCleanCSRGraph = sics::graph::miniclean::graphs::MiniCleanCSRGraph;
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;
  using VertexLabel = sics::graph::core::common::VertexLabel;

 public:
  PathMatcher(MiniCleanCSRGraph* miniclean_csr_graph)
      : miniclean_csr_graph_(miniclean_csr_graph){};
  ~PathMatcher() {}
  // For temporary usage.
  // It would be removed when `Graph Loader` API is available.
  void LoadGraph(const std::string& data_path);

  void LoadPatterns(const std::string& pattern_path);

  // For temporary usage.
  // It would be contained in `Graph Partitioner` or `Preprocessing` module.
  void BuildCandidateSet();

  // Match path patterns in the graph parallelly.
  void PathMatching(unsigned int parallelism);

  void PrintMatchedResults();

  std::vector<std::vector<std::vector<VertexID>>> get_results() {
    return matched_results_;
  }

 private:
  void PathMatchRecur(const std::vector<VertexLabel>& path_pattern,
                      size_t match_position,
                      const std::unordered_set<VertexID>& candidates,
                      std::vector<VertexID>* partial_results,
                      std::vector<std::vector<VertexID>>* results);

  MiniCleanCSRGraph* miniclean_csr_graph_;
  std::vector<std::vector<VertexLabel>> path_patterns_;
  std::vector<std::vector<std::vector<VertexID>>> matched_results_;
  std::vector<std::unordered_set<VertexID>> candidates_;
  VertexLabel num_label_ = 0;

  std::mutex mtx_;
};
}  // namespace sics::graph::miniclean::components

#endif  // MINICLEAN_COMPONENTS_PATH_MATCHER_H_
