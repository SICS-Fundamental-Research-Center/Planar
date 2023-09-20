#ifndef MINICLEAN_DATA_STRUCTURES_GCR_GCR_FACTORY_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_GCR_FACTORY_H_

#include <vector>

#include "miniclean/data_structures/gcr/path_rule.h"
#include "miniclean/data_structures/gcr/refactor_gcr.h"
#include "miniclean/data_structures/gcr/refactor_predicate.h"

namespace sics::graph::miniclean::data_structures::gcr {

class GCRFactory {
 private:
  using PathRule = sics::graph::miniclean::data_structures::gcr::PathRule;
  using GCR = sics::graph::miniclean::data_structures::gcr::refactor::GCR;
  using VariablePredicate =
      sics::graph::miniclean::data_structures::gcr::refactor::VariablePredicate;
  using ConcreteVariablePredicate = sics::graph::miniclean::data_structures::
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
  std::vector<GCR> InitializeGCRs(const GCR& gcr);

  // This function aims to extend GCRs:
  //   - merge the path_rule into the GCR
  //   - assign consequence predicates
  std::vector<GCR> MergeAndCompleteGCRs(const GCR& gcr,
                                        const PathRule& path_rule);

 private:
  void ConcretizeVariablePredicates(
      const GCR& gcr, const VariablePredicate& variable_predicate,
      std::vector<ConcreteVariablePredicate>* predicates,
      bool consider_consequence) const;

  void ExtendVariablePredicates(
      const GCR& gcr, std::vector<ConcreteVariablePredicate>& predicates,
      size_t start_id, std::vector<GCR>* gcrs) const;

  std::vector<VariablePredicate> consequence_predicates_;
  std::vector<VariablePredicate> variable_predicates_;
  size_t max_predicate_num_;
};
}  // namespace sics::graph::miniclean::data_structures::gcr

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_GCR_FACTORY_H_