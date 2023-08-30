#ifndef MINICLEAN_COMPONENTS_RULE_DISCOVERY_RULE_DISCOVERY_H_
#define MINICLEAN_COMPONENTS_RULE_DISCOVERY_RULE_DISCOVERY_H_

#include <string>
#include <unordered_map>

#include "miniclean/common/types.h"
#include "miniclean/data_structures/gcr/gcr.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::components::rule_discovery {

// TODO (bai-wenchao): Implement out-of-core version.
class RuleMiner {
 private:
  using ConstantPredicate =
      sics::graph::miniclean::data_structures::gcr::ConstantPredicate;
  using GCR = sics::graph::miniclean::data_structures::gcr::GCR;
  using MiniCleanCSRGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using PathInstance = sics::graph::miniclean::common::PathInstance;
  using PathPattern = sics::graph::miniclean::common::PathPattern;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VariablePredicate =
      sics::graph::miniclean::data_structures::gcr::VariablePredicate;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;

 public:
  RuleMiner(MiniCleanCSRGraph* graph) : graph_(graph) {}

  void LoadGraph(const std::string& graph_path);

  void LoadPathPatterns(const std::string& path_patterns_path);

  void LoadPathInstances(const std::string& path_instances_path);

  void LoadPredicates(const std::string& predicates_path);

  void InitGCRs();

  void RuleDiscovery();

 private:
  MiniCleanCSRGraph* graph_;
  std::vector<GCR> gcrs_;
  std::vector<PathPattern> path_patterns_;
  std::vector<std::vector<std::vector<VertexID>>> path_instances_;

  std::unordered_map<
      VertexLabel,
      std::unordered_map<VertexLabel, std::vector<VariablePredicate>>>
      variable_predicates_;
  std::unordered_map<VertexLabel, std::vector<ConstantPredicate>>
      constant_predicates_;
};

}  // namespace sics::graph::miniclean::components::rule_discovery

#endif  // MINICLEAN_COMPONENTS_RULE_DISCOVERY_RULE_DISCOVERY_H_