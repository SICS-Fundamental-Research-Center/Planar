#include "miniclean/data_structures/gcr/gcr_factory.h"

namespace sics::graph::miniclean::data_structures::gcr {

using PathRule = sics::graph::miniclean::data_structures::gcr::PathRule;
using GCR = sics::graph::miniclean::data_structures::gcr::refactor::GCR;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
using VariablePredicate =
    sics::graph::miniclean::data_structures::gcr::refactor::VariablePredicate;
using ConcreteVariablePredicate = std::pair<
    std::pair<std::pair<uint8_t, uint8_t>, std::pair<uint8_t, uint8_t>>,
    VariablePredicate>;

std::vector<GCR> GCRFactory::InitializeGCRs(GCR& gcr) {
  std::vector<GCR> gcrs;
  // Check number of preconditions.
  if (gcr.CountPreconditions() > max_predicate_num_ - 1) {
    LOG_WARN("GCRFactory::InitializeGCRs: too many preconditions.");
    return gcrs;
  }
  // Assign consequence.
  for (const auto& consequence : consequence_predicates_) {
    std::vector<ConcreteVariablePredicate> predicates;
    ConcretizeVariablePredicates(gcr, consequence, &predicates, false);
    for (const auto& predicate : predicates) {
      gcr.set_consequence(predicate);
      gcrs.push_back(gcr);
    }
  }
  // Assign variable predicates.
  for (const auto& variable : variable_predicates_) {
    std::vector<ConcreteVariablePredicate> predicates;
    ConcretizeVariablePredicates(gcr, variable, &predicates, true);
    for (const auto& c_gcr : gcrs) {
      ExtendVariablePredicates(c_gcr, predicates, 0, &gcrs);
    }
  }
  return gcrs;
}

std::vector<GCR> GCRFactory::MergeAndCompleteGCRs(const GCR& gcr,
                                                  const PathRule& path_rule) {
  // TODO
}

void GCRFactory::ConcretizeVariablePredicates(
    const GCR& gcr, const VariablePredicate& variable_predicate,
    std::vector<ConcreteVariablePredicate>* predicates,
    bool consider_consequence) const {
  VertexLabel lhs_label = variable_predicate.get_lhs_label();
  VertexLabel rhs_label = variable_predicate.get_rhs_label();
  for (size_t i = 0; i < gcr.get_left_star().size(); i++) {
    for (size_t j = 0; j < gcr.get_right_star().size(); j++) {
      // For each pair of paths.
      for (size_t k = 0; k < gcr.get_left_star()[i]->get_path_pattern().size();
           k++) {
        if (std::get<0>(gcr.get_left_star()[i]->get_path_pattern()[k]) !=
            lhs_label)
          continue;
        for (size_t l = 0;
             l < gcr.get_right_star()[j]->get_path_pattern().size(); l++) {
          if (std::get<0>(gcr.get_right_star()[j]->get_path_pattern()[l]) !=
              rhs_label)
            continue;
          if (gcr.IsCompatibleWith(gcr.ConcretizeVariablePredicate(
                                       variable_predicate, i, k, j, l),
                                   consider_consequence)) {
            predicates->push_back(gcr.ConcretizeVariablePredicate(
                variable_predicate, i, k, j, l));
          }
        }
      }
    }
  }
}

void GCRFactory::ExtendVariablePredicates(
    const GCR& gcr, std::vector<ConcreteVariablePredicate>& predicates,
    size_t start_id, std::vector<GCR>* gcrs) const {
  if (gcr.CountPreconditions() >= max_predicate_num_) return;

  GCR new_gcr = gcr;
  for (size_t i = start_id; i < predicates.size(); i++) {
    if (!gcr.IsCompatibleWith(predicates[i], true)) continue;
    new_gcr.AddVariablePredicateToBack(predicates[i]);
    gcrs->push_back(new_gcr);
    ExtendVariablePredicates(new_gcr, predicates, i + 1, gcrs);
    bool pop_status = new_gcr.PopVariablePredicateFromBack();
    if (!pop_status) {
      LOG_ERROR("GCRFactory::ExtendVariablePredicates: pop failed.");
    }
  }
}

}  // namespace sics::graph::miniclean::data_structures::gcr