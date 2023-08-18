#ifndef MINICLEAN_COMPONENTS_PATH_MATCHER_H_
#define MINICLEAN_COMPONENTS_PATH_MATCHER_H_

#include <set>
#include <vector>

#include "core/common/types.h"
#include "miniclean/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::components {

// TODO (bai-wenchao): Load pattern and graph from configuration. 
// (Hard-coded for temporary usage.)
class PathMatcher {
 private:
  using MiniCleanCSRGraph = sics::graph::miniclean::graphs::MiniCleanCSRGraph;
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;
  using VertexLabel = sics::graph::core::common::VertexLabel;

 public:
  PathMatcher(MiniCleanCSRGraph* miniclean_csr_graph,
              std::vector<std::vector<VertexLabel>> path_patterns,
              std::set<VertexID>* candidates,
              int num_label)
      : miniclean_csr_graph_(miniclean_csr_graph),
        path_patterns_(path_patterns),
        candidates_(candidates),
        num_label_(num_label){};

  // For temporary usage.
  // It would be removed when `Graph Loader` API is available.
  void LoadGraph(const std::string& data_path);

  // For temporary usage.
  // It would be contained in `Graph Partitioner` or `Preprocessing` module.
  void BuildCandidateSet(VertexLabel num_label);

  // Match path patterns in the graph.
  //   This function will call `PathMatching(path_pattern, results)` for each
  //   pattern and collect their results.
  void PathMatching();

  // Match a single path pattern in the graph.
  //   Input: graph, a single path pattern
  //   Output: matched path instances
  void PathMatching(const std::vector<VertexLabel>& path_pattern,
                    std::vector<std::vector<VertexID>>* results);
  
  void PrintMatchedResults();

  std::vector<std::vector<std::vector<VertexID>>> get_results() {
    return matched_results_;
  }

 private:
  void PathMatchRecur(const std::vector<VertexLabel>& path_pattern,
                        size_t match_position, std::set<VertexID>& candidates,
                        std::vector<VertexID>& partial_result,
                        std::vector<std::vector<VertexID>>* results);

  MiniCleanCSRGraph* miniclean_csr_graph_;
  std::vector<std::vector<VertexLabel>> path_patterns_;
  std::vector<std::vector<std::vector<VertexID>>> matched_results_;
  std::set<VertexID>* candidates_ = nullptr;
  int num_label_ = 0;

  std::mutex mtx_;
};
}  // namespace sics::graph::miniclean::components

#endif  // MINICLEAN_COMPONENTS_PATH_MATCHER_H_
