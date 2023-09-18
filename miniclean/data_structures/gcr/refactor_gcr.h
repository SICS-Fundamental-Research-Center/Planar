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
  // Each tuple contains:
  //   - the first element is the path pattern id (in the star pattern).
  //   - the second element is the vertex id (in the path pattern).
  //   - the third element is the predicate.
  using ConcreteVariablePredicate =
      std::pair<std::tuple<uint8_t, uint8_t, VariablePredicate>,
                std::tuple<uint8_t, uint8_t, VariablePredicate>>;
  using ConcreteConsequence =
      std::pair<std::tuple<uint8_t, uint8_t, VariablePredicate>,
                std::tuple<uint8_t, uint8_t, VariablePredicate>>;

 public:
  GCR() = default;
  GCR(StarRule left_star, StarRule right_star)
      : left_star_(left_star), right_star_(right_star) {}

  void AddVariablePredicateToBack(
      ConcreteVariablePredicate variable_predicate) {
    variable_predicates_.push_back(variable_predicate);
  }

  void set_consequence(ConcreteConsequence consequence) {
    consequence_ = consequence;
  }

  StarRule get_left_star() const { return left_star_; }
  StarRule get_right_star() const { return right_star_; }

  std::vector<ConcreteVariablePredicate> get_variable_predicates() const {
    return variable_predicates_;
  }

  ConcreteConsequence get_consequence() const { return consequence_; }

  std::pair<size_t, size_t> ComputeMatchAndSupport() const;

 private:
  StarRule left_star_;
  StarRule right_star_;

  std::vector<ConcreteVariablePredicate> variable_predicates_;
  ConcreteConsequence consequence_;
};

}  // namespace sics::graph::miniclean::data_structures::gcr::refactor

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_R_GCR_H_