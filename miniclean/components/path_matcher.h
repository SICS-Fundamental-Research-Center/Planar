#ifndef MINICLEAN_COMPONENTS_PATH_MATCHER_H_
#define MINICLEAN_COMPONENTS_PATH_MATCHER_H_

#include <set>
#include <vector>

#include "core/common/types.h"
#include "miniclean/graphs/miniclean_csr_graph.h"
#include "miniclean/graphs/miniclean_csr_graph_config.h"

namespace sics::graph::miniclean::components {

class PathMatcher {
 private:
  using MinicleanCSRGraph = sics::graph::miniclean::graphs::MinicleanCSRGraph;
  using MinicleanCSRGraphConfig =
      sics::graph::miniclean::graphs::MinicleanCSRGraphConfig;
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;
  using VertexLabel = sics::graph::core::common::VertexLabel;

 public:
  PathMatcher(MinicleanCSRGraph&& miniclean_csr_graph,
              std::vector<std::vector<VertexLabel>>&& path_patterns,
              MinicleanCSRGraphConfig&& miniclean_csr_graph_config,
              std::set<VertexID>* candidates,
              int num_label)
      : miniclean_csr_graph_(std::move(miniclean_csr_graph)),
        path_patterns_(std::move(path_patterns)),
        miniclean_csr_graph_config_(std::move(miniclean_csr_graph_config)),
        candidates_(candidates),
        num_label_(num_label){};

  // For temporary usage.
  // It would be removed when `Graph Loader` API is available.
  void LoadGraph(const std::string& data_path);

  // For temporary usage.
  // It would be contained in `Graph Partitioner` or `Preprocessing` module.
  void BuildCandidateSet(VertexLabel num_label);

  // Match path patterns in the graph.
  //   Input: graph, a set of path patterns
  //   Output: matched path instances
  void PathMatching();

  // Match a single path pattern in the graph.
  //   Input: graph, a single path pattern
  //   Output: matched path instances
  void PathMatching(const std::vector<VertexLabel>& path_pattern,
                    std::vector<std::vector<VertexID>>& results);
  
  void PrintMatchedResults();

  std::vector<std::vector<std::vector<VertexID>>> get_results() {
    return matched_results_;
  }

 private:
  void path_match_recur(const std::vector<VertexLabel>& path_pattern,
                        size_t match_position, std::set<VertexID>& candidates,
                        std::vector<VertexID>& partial_result,
                        std::vector<std::vector<VertexID>>& results);

  MinicleanCSRGraph miniclean_csr_graph_;
  std::vector<std::vector<VertexLabel>> path_patterns_;
  MinicleanCSRGraphConfig miniclean_csr_graph_config_;
  std::vector<std::vector<std::vector<VertexID>>> matched_results_;
  std::set<VertexID>* candidates_ = nullptr;
  int num_label_ = 0;
};
}  // namespace sics::graph::miniclean::components

#endif  // MINICLEAN_COMPONENTS_PATH_MATCHER_H_
