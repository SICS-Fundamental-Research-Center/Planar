#ifndef MINICLEAN_COMPONENTS_RULE_DISCOVERY_R_RULE_MINER_H_
#define MINICLEAN_COMPONENTS_RULE_DISCOVERY_R_RULE_MINER_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "miniclean/common/types.h"
#include "miniclean/components/preprocessor/index_metadata.h"
#include "miniclean/data_structures/gcr/path_rule.h"
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
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
  using IndexMetadata =
      sics::graph::miniclean::components::preprocessor::IndexMetadata;
  using PathRule = sics::graph::miniclean::data_structures::gcr::PathRule;

 public:
  RuleMiner(MiniCleanCSRGraph* graph) : graph_(graph) {}

  void LoadGraph(const std::string& graph_path);
  void LoadPathInstances(const std::string& path_instances_path);
  // Path rule contains:
  //   - a path pattern
  //   - several constant predicates on its non-center vertices.
  // It's used to compose with a GCR.
  void LoadPathRules(const std::string& workspace_path);
  void LoadIndexMetadata(const std::string& index_metadata_path);

  void InitPathRules();

 private:
  void LoadPathPatterns(const std::string& path_patterns_path);
  void LoadPredicates(const std::string& predicates_path);
  void InitPathRulesRecur(PathRule& path_rule, size_t pattern_id, size_t index);

 private:
  MiniCleanCSRGraph* graph_;
  std::vector<PathPattern> path_patterns_;
  std::vector<std::vector<std::vector<VertexID>>> path_instances_;
  std::unordered_map<
      VertexLabel,
      std::unordered_map<VertexAttributeID, std::vector<ConstantPredicate>>>
      constant_predicates_;
  IndexMetadata index_metadata_;
  std::vector<std::vector<PathRule>> path_rules_;

  uint8_t max_predicate_num_ = 3;
};
}  // namespace sics::graph::miniclean::components::rule_discovery::refactor

#endif  // MINICLEAN_COMPONENTS_RULE_DISCOVERY_R_RULE_MINER_H_