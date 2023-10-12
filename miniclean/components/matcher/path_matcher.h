#ifndef MINICLEAN_COMPONENTS_MATCHER_PATH_MATCHER_H_
#define MINICLEAN_COMPONENTS_MATCHER_PATH_MATCHER_H_

#include <yaml-cpp/yaml.h>

#include <map>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "core/common/multithreading/task.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::components::matcher {

class PathMatcher {
 private:
  using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;
  using GraphID = sics::graph::miniclean::common::GraphID;
  using MiniCleanCSRGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using PathPattern = sics::graph::miniclean::common::PathPattern;
  using PathPatternID = sics::graph::miniclean::common::PathPatternID;
  using TaskPackage = sics::graph::core::common::TaskPackage;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;

 public:
  PathMatcher(MiniCleanCSRGraph* miniclean_csr_graph)
      : miniclean_csr_graph_(miniclean_csr_graph) {}
  ~PathMatcher() {}
  // For temporary usage.
  // It would be removed when `Graph Loader` API is available.
  void LoadGraph(const std::string& data_path);

  void LoadPatterns(const std::string& pattern_path);

  // Split the path matching tasks into `parallelism` parts.
  void GroupTasks(size_t num_tasks,
                  std::vector<std::vector<VertexID>>* partial_result_pool,
                  TaskPackage* task_package);

  // Match path patterns in the graph parallelly.
  void PathMatching(unsigned int parallelism, unsigned int num_tasks);

  // Write matched patterns back to disk.
  void WriteResultsToPath(const std::string& result_path);

  std::vector<std::list<std::vector<VertexID>>> get_results() {
    return matched_results_;
  }

 private:
  void PathMatchRecur(const PathPattern& path_pattern, size_t match_position,
                      const std::vector<VertexID>& candidates,
                      std::vector<VertexID>* partial_results,
                      std::list<std::vector<VertexID>>* results);

  MiniCleanCSRGraph* miniclean_csr_graph_;
  std::vector<PathPattern> path_patterns_;
  std::vector<std::list<std::vector<VertexID>>> matched_results_;
  std::vector<std::vector<PathPatternID>> vertex_label_to_pattern_id_;

  double exe_t0 = 10000, write_back_t0 = 10000, exe_t1 = 0, write_back_t1 = 0;

  std::mutex mtx_;
  std::mutex dur_mtx_;
};
}  // namespace sics::graph::miniclean::components::matcher

namespace YAML {

using EdgePattern = sics::graph::miniclean::common::EdgePattern;
using PathPattern = sics::graph::miniclean::common::PathPattern;
using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
template <>
struct convert<PathPattern> {
  static Node encode(const PathPattern& path_pattern) {
    std::vector<uint8_t> path_pattern_node;
    path_pattern_node.reserve(path_pattern.size() * 2 - 1);
    for (size_t i = 0; i < path_pattern.size() - 1; ++i) {
      path_pattern_node.push_back(std::get<0>(path_pattern[i]));
      path_pattern_node.push_back(std::get<1>(path_pattern[i]));
    }
    path_pattern_node.push_back(
        std::get<0>(path_pattern[path_pattern.size() - 1]));
    Node node(path_pattern_node);
    return node;
  }

  static bool decode(const Node& node, PathPattern& path_pattern) {
    if (!node.IsSequence()) {
      return false;
    }
    path_pattern.reserve(node.size() / 2 + 1);
    for (size_t i = 0; i < node.size() - 1; i += 2) {
      path_pattern.emplace_back(node[i].as<VertexLabel>(),
                                node[i + 1].as<EdgeLabel>(),
                                node[i + 2].as<VertexLabel>());
    }
    path_pattern.emplace_back(node[node.size() - 1].as<VertexLabel>(),
                              MAX_EDGE_LABEL, MAX_VERTEX_LABEL);
    return true;
  }
};
}  // namespace YAML

#endif  // MINICLEAN_COMPONENTS_PATH_MATCHER_H_
