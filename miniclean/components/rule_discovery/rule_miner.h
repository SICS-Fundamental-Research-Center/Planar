#ifndef MINICLEAN_COMPONENTS_RULE_DISCOVERY_RULE_MINER_H_
#define MINICLEAN_COMPONENTS_RULE_DISCOVERY_RULE_MINER_H_

#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "miniclean/common/config.h"
#include "miniclean/common/types.h"
#include "miniclean/components/preprocessor/index_collection.h"
#include "miniclean/data_structures/gcr/gcr.h"
#include "miniclean/data_structures/gcr/path_rule.h"
#include "miniclean/data_structures/gcr/predicate.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::components::rule_discovery {

class RuleMiner {
 private:
  using PathPattern = sics::graph::miniclean::common::PathPattern;
  using PathPatternID = sics::graph::miniclean::common::PathPatternID;
  using ConstantPredicate =
      sics::graph::miniclean::data_structures::gcr::ConstantPredicate;
  using MiniCleanCSRGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
  using VertexAttributeValue =
      sics::graph::miniclean::common::VertexAttributeValue;
  using PathRule = sics::graph::miniclean::data_structures::gcr::PathRule;
  using StarRule = sics::graph::miniclean::data_structures::gcr::StarRule;
  using VariablePredicate =
      sics::graph::miniclean::data_structures::gcr::VariablePredicate;
  using ConcreteVariablePredicate =
      sics::graph::miniclean::data_structures::gcr::ConcreteVariablePredicate;
  using GCR = sics::graph::miniclean::data_structures::gcr::GCR;
  using IndexCollection =
      sics::graph::miniclean::components::preprocessor::IndexCollection;
  // Dim. #1: vertex attribute id.
  // Dim. #2: path rules with different vertex attribute value.
  using PathRuleUnits = std::vector<std::vector<PathRule>>;
  // Dim. #1: path pattern id.
  // Dim. #2: vertex index in the path.
  using PathRuleUnitContainer = std::vector<std::vector<PathRuleUnits>>;
  // Dim. #1: vertex attribute id.
  // Dim. #2：star rules with different attribute value.
  using StarRuleUnits = std::vector<std::vector<StarRule>>;
  // Dim. #1: vertex label id.
  using StarRuleUnitContainer = std::vector<StarRuleUnits>;
  using ConstantPredicateContainer =
      std::map<VertexLabel, std::map<VertexAttributeID, ConstantPredicate>>;
  using Configurations = sics::graph::miniclean::common::Configurations;
  using GCRVerticalExtension =
      sics::graph::miniclean::data_structures::gcr::GCRVerticalExtension;
  using GCRHorizontalExtension =
      sics::graph::miniclean::data_structures::gcr::GCRHorizontalExtension;

 public:
  RuleMiner(MiniCleanCSRGraph& graph) : graph_(graph) {}

  void LoadGraph(const std::string& graph_path);
  void LoadIndexCollection(const std::string& workspace_path);
  void LoadPathInstances(const std::string& path_instances_path);
  // Path rule contains:
  //   - a path pattern
  //   - several constant predicates on its non-center vertices.
  // It's used to compose with a GCR.
  void PrepareGCRComponents(const std::string& workspace_path);

  void MineGCRs();

 private:
  void LoadPathPatterns(const std::string& path_patterns_path);
  void LoadPredicates(const std::string& predicates_path);
  void InitStarRuleUnitContainer();
  void InitPathRuleUnitContainer();
  template <typename T>
  void ComposeUnits(const std::vector<std::vector<T>>& unit_container,
                    size_t max_item, bool check_support, size_t start_idx,
                    std::vector<T>* intermediate_result,
                    std::vector<T>* composed_results) const {
    // Check return condition.
    size_t predicate_count = 0;
    for (const auto& result : *intermediate_result) {
      predicate_count += result.get_constant_predicates().size();
    }
    if (predicate_count >= max_item) return;

    for (size_t i = start_idx; i < unit_container.size(); i++) {
      // Compose intermediate result with unit_container[i].
      for (size_t j = 0; j < unit_container[i].size(); j++) {
        intermediate_result->emplace_back(unit_container[i][j]);
        // Construct the composed item.
        T composed_item = (*intermediate_result)[0];
        for (size_t k = 1; k < intermediate_result->size(); k++) {
          composed_item.ComposeWith((*intermediate_result)[k]);
        }
        // Check support.
        if (check_support) {
          size_t support = composed_item.ComputeInitSupport();
          if (support < Configurations::Get()->star_support_threshold_) {
            intermediate_result->pop_back();
            continue;
          }
        }
        composed_results->emplace_back(composed_item);
        ComposeUnits(unit_container, max_item, check_support, i + 1,
                     intermediate_result, composed_results);
        intermediate_result->pop_back();
      }
    }
  }
  void ExtendGCR(GCR* gcr) const;
  std::vector<GCRVerticalExtension> ComputeVerticalExtensions(
      const GCR& gcr) const;
  std::vector<GCRHorizontalExtension> ComputeHorizontalExtensions(
      const GCR& gcr, bool from_left) const;
  std::vector<ConcreteVariablePredicate> InstantiateVariablePredicates(
      const GCR& gcr,
      const std::vector<VariablePredicate>& variable_predicates) const;
  std::vector<GCRHorizontalExtension> ExtendVariablePredicates(
      const GCR& gcr,
      const std::vector<ConcreteVariablePredicate>& consequences) const;
  std::vector<GCRHorizontalExtension> MergeHorizontalExtensions(
      const GCR& gcr,
      const std::vector<ConcreteVariablePredicate>& c_consequences,
      std::vector<std::vector<ConcreteVariablePredicate>> c_variable_predicates,
      size_t available_var_pred_num) const;
  void EnumerateValidVariablePredicates(
      const std::vector<ConcreteVariablePredicate>& variable_predicates,
      size_t start_idx, size_t max_item_num,
      std::vector<ConcreteVariablePredicate>* intermediate_result,
      std::vector<std::vector<ConcreteVariablePredicate>>*
          valid_variable_predicates) const;
  size_t ComputeCombinationNum(size_t n, size_t k) const;

 private:
  MiniCleanCSRGraph& graph_;
  std::vector<PathPattern> path_patterns_;
  IndexCollection index_collection_;
  ConstantPredicateContainer constant_predicate_container_;
  // TODO: discuss the design of consequence predicates.
  //   - Is it enough only declare the consequence as `VariablePredicate`?
  std::vector<VariablePredicate> consequence_predicates_;
  std::vector<VariablePredicate> variable_predicates_;
  StarRuleUnitContainer star_rule_unit_container_;
  PathRuleUnitContainer path_rule_unit_container_;
  std::vector<std::vector<StarRule>> star_rules_;
  std::vector<std::vector<PathRule>> path_rules_;

  std::vector<GCR> varified_gcrs_;
};
}  // namespace sics::graph::miniclean::components::rule_discovery

#endif  // MINICLEAN_COMPONENTS_RULE_DISCOVERY_RULE_MINER_H_