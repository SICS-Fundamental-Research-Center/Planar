#ifndef MINICLEAN_DATA_STRUCTURES_GCR_GCR_FACTORY_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_GCR_FACTORY_H_

#include <vector>

#include "miniclean/data_structures/gcr/path_rule.h"
#include "miniclean/data_structures/gcr/refactor_gcr.h"
#include "miniclean/data_structures/gcr/refactor_predicate.h"

namespace xyz::graph::miniclean::data_structures::gcr {

class GCRFactory {
 private:
  using PathRule = xyz::graph::miniclean::data_structures::gcr::PathRule;
  using GCR = xyz::graph::miniclean::data_structures::gcr::refactor::GCR;
  using VariablePredicate =
      xyz::graph::miniclean::data_structures::gcr::refactor::VariablePredicate;
  using ConcreteVariablePredicate = xyz::graph::miniclean::data_structures::
      gcr::refactor::ConcreteVariablePredicate;

 public:
  GCRFactory() = default;
  GCRFactory(const std::vector<VariablePredicate>& consequence_predicates,
             const std::vector<VariablePredicate>& variable_predicates,
             size_t max_predicate_num)
      : consequence_predicates_(consequence_predicates),
        variable_predicates_(variable_predicates),
        max_predicate_num_(max_predicate_num) {}

  // This function aims to initialize GCRs:
  //   - assign consequence
  //   - assign variable predicates
  // The input GCR only carries constant predicates.
  bool InitializeGCRs(const GCR& gcr, bool added_to_left_star,
                      std::vector<GCR>* complete_gcrs);

  // This function aims to extend GCRs:
  //   - merge the path_rule into the GCR
  //   - assign consequence predicates
  bool MergeAndCompleteGCRs(const GCR& gcr, PathRule* path_rule,
                            size_t max_path_num,
                            std::vector<GCR>* complete_gcrs);

 private:
  // This function aims to concretize variable predicates.
  // The concrete predicates are stored in `predicates`.
  // The function returns true if the concretization is successful.
  //   (i.e., size of `predicates` is greater than 0)
  bool ConcretizeVariablePredicates(
      const GCR& gcr, const VariablePredicate& variable_predicate,
      bool added_to_left_star, bool consider_consequence,
      std::vector<ConcreteVariablePredicate>* predicates) const;

  void ExtendVariablePredicates(
      const GCR& gcr, std::vector<ConcreteVariablePredicate>& predicates,
      size_t start_id, std::vector<GCR>* gcrs) const;

  std::vector<VariablePredicate> consequence_predicates_;
  std::vector<VariablePredicate> variable_predicates_;
  size_t max_predicate_num_;
};
}  // namespace xyz::graph::miniclean::data_structures::gcr

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_GCR_FACTORY_H_