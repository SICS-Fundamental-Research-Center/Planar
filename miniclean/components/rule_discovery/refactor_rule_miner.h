#ifndef MINICLEAN_COMPONENTS_RULE_DISCOVERY_R_RULE_MINER_H_
#define MINICLEAN_COMPONENTS_RULE_DISCOVERY_R_RULE_MINER_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "miniclean/common/types.h"
#include "miniclean/data_structures/gcr/refactor_predicate.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::components::rule_discovery::refactor {

class RuleMiner {
 private:
  using PathPattern = sics::graph::miniclean::common::PathPattern;
  using ConstantPredicate =
      sics::graph::miniclean::data_structures::gcr::refactor::ConstantPredicate;
  using MiniCleanCSRGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;

 public:
  RuleMiner(MiniCleanCSRGraph* graph) : graph_(graph) {}

  void LoadGraph(const std::string& graph_path);
  void LoadPathInstances(const std::string& path_instances_path);
  // Path rule contains:
  //   - a path pattern
  //   - several constant predicates on its non-center vertices.
  // It's used to compose with a GCR.
  void LoadPathRules(const std::string& workspace_path);

 private:
  void LoadPathPatterns(const std::string& path_patterns_path);
  void LoadPredicates(const std::string& predicates_path);

 private:
  MiniCleanCSRGraph* graph_;
  std::vector<PathPattern> path_patterns_;
  std::vector<std::vector<std::vector<VertexID>>> path_instances_;
  std::unordered_map<VertexLabel, std::vector<ConstantPredicate>>
      constant_predicates_;
};
}  // namespace sics::graph::miniclean::components::rule_discovery::refactor

#endif  // MINICLEAN_COMPONENTS_RULE_DISCOVERY_R_RULE_MINER_H_