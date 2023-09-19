#ifndef MINICLEAN_DATA_STRUCTURES_GCR_R_GCR_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_R_GCR_H_

#include "miniclean/data_structures/gcr/path_rule.h"
#include "miniclean/data_structures/gcr/refactor_predicate.h"

namespace sics::graph::miniclean::data_structures::gcr::refactor {

class GCR {
 private:
  using PathRule = sics::graph::miniclean::data_structures::gcr::PathRule;
  using StarRule = std::vector<PathRule*>;
  using VariablePredicate =
      sics::graph::miniclean::data_structures::gcr::refactor::VariablePredicate;
  // VariablePredicate:
  //  - first: localtion info.
  //    - first: lhs
  //      - first: path index
  //      - second: vertex index
  //    - second: rhs
  //      - first: path index
  //      - second: vertex index
  //  - second: predicate.
  using ConcreteVariablePredicate = std::pair<
      std::pair<std::pair<uint8_t, uint8_t>, std::pair<uint8_t, uint8_t>>,
      VariablePredicate>;
  using ConcreteConsequence = std::pair<
      std::pair<std::pair<uint8_t, uint8_t>, std::pair<uint8_t, uint8_t>>,
      VariablePredicate>;

 public:
  GCR(StarRule left_star, StarRule right_star)
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

  void set_consequence(ConcreteConsequence consequence) {
    consequence_ = consequence;
  }

  const StarRule& get_left_star() const { return left_star_; }
  const StarRule& get_right_star() const { return right_star_; }

  const std::vector<ConcreteVariablePredicate>& get_variable_predicates()
      const {
    return variable_predicates_;
  }

  const ConcreteConsequence& get_consequence() const { return consequence_; }

  ConcreteVariablePredicate ConcretizeVariablePredicate(
      const VariablePredicate& variable_predicate, uint8_t left_path_index,
      uint8_t left_vertex_index, uint8_t right_path_index,
      uint8_t right_vertex_index) const {
    return std::make_pair(
        std::make_pair(std::make_pair(left_path_index, left_vertex_index),
                       std::make_pair(right_path_index, right_vertex_index)),
        variable_predicate);
  }

  std::pair<size_t, size_t> ComputeMatchAndSupport() const;

  // Return the number of precondition predicates.
  size_t CountPreconditions() const;

  bool IsCompatibleWith(const ConcreteVariablePredicate& variable_predicate,
                        bool consider_consequence) const;

 private:
  StarRule left_star_;
  StarRule right_star_;

  std::vector<ConcreteVariablePredicate> variable_predicates_;
  ConcreteConsequence consequence_;
};

}  // namespace sics::graph::miniclean::data_structures::gcr::refactor

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_R_GCR_H_