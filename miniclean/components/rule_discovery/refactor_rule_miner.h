#ifndef MINICLEAN_COMPONENTS_RULE_DISCOVERY_R_RULE_MINER_H_
#define MINICLEAN_COMPONENTS_RULE_DISCOVERY_R_RULE_MINER_H_

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "miniclean/common/config.h"
#include "miniclean/common/types.h"
#include "miniclean/components/preprocessor/index_collection.h"
#include "miniclean/components/preprocessor/index_metadata.h"
#include "miniclean/data_structures/gcr/gcr_factory.h"
#include "miniclean/data_structures/gcr/path_rule.h"
#include "miniclean/data_structures/gcr/refactor_gcr.h"
#include "miniclean/data_structures/gcr/refactor_predicate.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::components::rule_discovery::refactor {

class RuleMiner {
 private:
  using PathPattern = sics::graph::miniclean::common::PathPattern;
  using PathPatternID = sics::graph::miniclean::common::PathPatternID;
  using ConstantPredicate =
      sics::graph::miniclean::data_structures::gcr::refactor::ConstantPredicate;
  using MiniCleanCSRGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
  using VertexAttributeValue =
      sics::graph::miniclean::common::VertexAttributeValue;
  using IndexMetadata =
      sics::graph::miniclean::components::preprocessor::IndexMetadata;
  using PathRule = sics::graph::miniclean::data_structures::gcr::PathRule;
  using StarRule = sics::graph::miniclean::data_structures::gcr::StarRule;
  using VariablePredicate =
      sics::graph::miniclean::data_structures::gcr::refactor::VariablePredicate;
  using GCRFactory = sics::graph::miniclean::data_structures::gcr::GCRFactory;
  using IndexCollection =
      sics::graph::miniclean::components::preprocessor::IndexCollection;
  using PathRuleUnits = std::map<VertexAttributeID, std::vector<PathRule>>;
  // First dimension: path pattern id.
  // Second dimension: vertex index in the path.
  using PathRuleUnitContainer = std::vector<std::vector<PathRuleUnits>>;
  using GCR = sics::graph::miniclean::data_structures::gcr::refactor::GCR;
  using StarRuleContainer =
      std::map<VertexLabel, std::map<VertexAttributeID, std::vector<StarRule>>>;
  using ConstantPredicateContainer =
      std::map<VertexLabel, std::map<VertexAttributeID, ConstantPredicate>>;
  using Configurations = sics::graph::miniclean::common::Configurations;

 public:
  RuleMiner(MiniCleanCSRGraph* graph) : graph_(graph) {}

  void LoadGraph(const std::string& graph_path);
  void LoadIndexCollection(const std::string& workspace_path);

  void LoadPathInstances(const std::string& path_instances_path);
  // Path rule contains:
  //   - a path pattern
  //   - several constant predicates on its non-center vertices.
  // It's used to compose with a GCR.
  void PrepareGCRComponents(const std::string& workspace_path);

  void InitPathRuleUnits();

  void MineGCRs();

 private:
  void LoadPathPatterns(const std::string& path_patterns_path);
  void LoadPredicates(const std::string& predicates_path);
  void InitStarRuleContainer();
  void InitPathRuleUnitContainer();
  void ExtendPathRules(size_t pattern_id);
  void ExtendGCR(const GCR& gcr, size_t start_pattern_id, size_t start_rule_id,
                 VertexLabel left_center_label, VertexLabel right_center_label);

 private:
  MiniCleanCSRGraph* graph_;
  std::vector<PathPattern> path_patterns_;
  IndexCollection index_collection_;
  ConstantPredicateContainer constant_predicate_container_;
  // TODO: discuss the design of consequence predicates.
  //   - Is it enough only declare the consequence as `VariablePredicate`?
  std::vector<VariablePredicate> consequence_predicates_;
  std::vector<VariablePredicate> variable_predicates_;
  StarRuleContainer star_rule_container_;

  std::vector<std::vector<PathRule>> path_rules_;
  PathRuleUnitContainer path_rule_unit_container_;

  std::vector<GCR> varified_gcrs_;
  GCRFactory gcr_factory_;
};
}  // namespace sics::graph::miniclean::components::rule_discovery::refactor

#endif  // MINICLEAN_COMPONENTS_RULE_DISCOVERY_R_RULE_MINER_H_