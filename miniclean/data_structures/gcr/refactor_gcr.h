#ifndef MINICLEAN_DATA_STRUCTURES_GCR_R_GCR_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_R_GCR_H_

#include "miniclean/data_structures/gcr/path_rule.h"
#include "miniclean/data_structures/gcr/refactor_predicate.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::data_structures::gcr::refactor {

class GCR {
 private:
  using PathRule = sics::graph::miniclean::data_structures::gcr::PathRule;
  using StarRule = std::vector<PathRule*>;
  using VariablePredicate =
      sics::graph::miniclean::data_structures::gcr::refactor::VariablePredicate;
  using ConcreteVariablePredicate = sics::graph::miniclean::data_structures::
      gcr::refactor::ConcreteVariablePredicate;
  using StarBitmap =
      sics::graph::miniclean::components::preprocessor::StarBitmap;
  using MiniCleanCSRGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using PathPattern = sics::graph::miniclean::common::PathPattern;
  using PathRuleUnitContainer =
      std::vector<std::vector<std::vector<std::vector<std::vector<PathRule>>>>>;

 public:
  GCR(const StarRule& left_star, const StarRule& right_star)
      : left_star_(left_star), right_star_(right_star) {}

  void AddVariablePredicateToBack(
      const ConcreteVariablePredicate& variable_predicate) {
    variable_predicates_.push_back(variable_predicate);
  }

  bool PopVariablePredicateFromBack() {
    if (!variable_predicates_.empty()) {
      variable_predicates_.pop_back();
      return true;
    }
    return false;
  }

  void AddPathRuleToLeftStar(PathRule* path_rule) {
    left_star_.push_back(path_rule);
  }

  void AddPathRuleToRigthStar(PathRule* path_rule) {
    right_star_.push_back(path_rule);
  }

  void set_consequence(const ConcreteVariablePredicate& consequence) {
    consequence_ = consequence;
  }

  const StarRule& get_left_star() const { return left_star_; }
  const StarRule& get_right_star() const { return right_star_; }

  const std::vector<ConcreteVariablePredicate>& get_variable_predicates()
      const {
    return variable_predicates_;
  }

  const ConcreteVariablePredicate& get_consequence() const {
    return consequence_;
  }

  ConcreteVariablePredicate ConcretizeVariablePredicate(
      const VariablePredicate& variable_predicate, uint8_t left_path_index,
      uint8_t left_vertex_index, uint8_t right_path_index,
      uint8_t right_vertex_index) const {
    ConcreteVariablePredicate concrete_variable_predicate(
        variable_predicate, left_path_index, left_vertex_index,
        right_path_index, right_vertex_index);
    return concrete_variable_predicate;
  }

  std::pair<size_t, size_t> ComputeMatchAndSupport(
      MiniCleanCSRGraph* graph,
      PathRuleUnitContainer& path_rule_unit_container) const;

  // Return the number of precondition predicates.
  size_t CountPreconditions() const;

  bool IsCompatibleWith(const ConcreteVariablePredicate& variable_predicate,
                        bool consider_consequence) const;

  void StarRuleCheck(StarRule star_rule, MiniCleanCSRGraph* graph,
                     StarBitmap* star_bitmap) const;

  bool PathMatching(PathPattern path_pattern, MiniCleanCSRGraph* graph,
                    size_t vertex_id, size_t edge_id) const;

 private:
  std::vector<std::vector<std::pair<size_t, size_t>>>
  ComputeVariablePredicateInstances() const;
  void EnumerateVariablePredicateInstances(
      std::vector<std::vector<std::pair<size_t, size_t>>>& value_pair_vec,
      size_t variable_predicate_index,
      std::vector<std::pair<size_t, size_t>>& current_value_pair_vec,
      std::vector<std::vector<std::pair<size_t, size_t>>>* var_pred_instances)
      const;
  std::vector<std::pair<size_t, size_t>> ComputeAttributeValuePair(
      const ConcreteVariablePredicate& variable_predicate) const;
  void UpdateBitmapByVariablePredicate(ConcreteVariablePredicate predicate,
                                       size_t left_attribute_value,
                                       size_t right_attribute_value,
                                       PathRuleUnitContainer& container,
                                       StarBitmap* left_bitmap,
                                       StarBitmap* right_bitmap) const;

  StarRule left_star_;
  StarRule right_star_;

  std::vector<ConcreteVariablePredicate> variable_predicates_;
  ConcreteVariablePredicate consequence_;
};

}  // namespace sics::graph::miniclean::data_structures::gcr::refactor

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_R_GCR_H_