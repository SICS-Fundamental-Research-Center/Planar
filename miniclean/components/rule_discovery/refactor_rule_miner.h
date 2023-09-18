#ifndef MINICLEAN_COMPONENTS_RULE_DISCOVERY_R_RULE_MINER_H_
#define MINICLEAN_COMPONENTS_RULE_DISCOVERY_R_RULE_MINER_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "miniclean/common/types.h"
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
  using StarRule = std::vector<PathRule*>;
  using VariablePredicate =
      sics::graph::miniclean::data_structures::gcr::refactor::VariablePredicate;
  using GCRFactory = sics::graph::miniclean::data_structures::gcr::GCRFactory;
  // The first dimension is the path pattern id.
  // The second dimension is the vertex id (indicates the vertex that carries
  // predicate).
  // The third dimension is the attribute id.
  // The fourth dimension is the value of attribute.
  // The fifth dimension is the operator type.
  using PathRuleUnitContainer =
      std::vector<std::vector<std::vector<std::vector<std::vector<PathRule>>>>>;
  using GCR = sics::graph::miniclean::data_structures::gcr::refactor::GCR;

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

  void InitPathRuleUnits();

  void MiningGCRs();

 private:
  void LoadPathPatterns(const std::string& path_patterns_path);
  void LoadPredicates(const std::string& predicates_path);
  void ExtendPathRules(size_t pattern_id);
  void ExtendGCR(GCR gcr, size_t start_pattern_id, size_t start_rule_id,
                 VertexLabel left_center_label, VertexLabel right_center_label);

 private:
  MiniCleanCSRGraph* graph_;
  std::vector<PathPattern> path_patterns_;
  std::vector<std::vector<std::vector<VertexID>>> path_instances_;
  std::unordered_map<
      VertexLabel,
      std::unordered_map<VertexAttributeID, std::vector<ConstantPredicate>>>
      constant_predicates_;
  // TODO: discuss the design of consequence predicates.
  //   - Is it enough only declare the consequence as `VariablePredicate`?
  std::vector<VariablePredicate> consequence_predicates_;
  std::vector<VariablePredicate> variable_predicates_;
  IndexMetadata index_metadata_;
  std::vector<std::vector<PathRule>> path_rules_;
  PathRuleUnitContainer path_rule_unit_container_;

  std::vector<GCR> varified_gcrs_;
  GCRFactory gcr_factory_;

  uint8_t max_predicate_num_ = 3;
};
}  // namespace sics::graph::miniclean::components::rule_discovery::refactor

#endif  // MINICLEAN_COMPONENTS_RULE_DISCOVERY_R_RULE_MINER_H_