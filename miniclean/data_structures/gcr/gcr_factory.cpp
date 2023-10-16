#include "miniclean/data_structures/gcr/gcr_factory.h"

namespace sics::graph::miniclean::data_structures::gcr {

using GCR = sics::graph::miniclean::data_structures::gcr::refactor::GCR;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;

bool GCRFactory::InitializeGCRs(const GCR& gcr, bool added_to_left_star,
                                std::vector<GCR>* complete_gcrs) {
  // Check number of preconditions.
  if (gcr.CountPreconditions() > max_predicate_num_ - 1) {
    return false;
  }
  std::vector<ConcreteVariablePredicate> c_predicates;
  // Assign consequence.
  for (const auto& consequence : consequence_predicates_) {
    ConcretizeVariablePredicates(gcr, consequence, added_to_left_star, false,
                                 &c_predicates);
  }
  const size_t prev_size = complete_gcrs->size();
  complete_gcrs->resize(prev_size + c_predicates.size(), gcr);
  for (size_t i = 0; i < c_predicates.size(); i++) {
    complete_gcrs->at(prev_size + i).set_consequence(c_predicates[i]);
  }
  // Assign variable predicates.
  for (const auto& variable : variable_predicates_) {
    std::vector<ConcreteVariablePredicate> v_predicates;
    bool concretize_status = ConcretizeVariablePredicates(
        gcr, variable, added_to_left_star, false, &v_predicates);
    if (!concretize_status) continue;
    for (size_t i = 0; i < c_predicates.size(); i++) {
      ExtendVariablePredicates(complete_gcrs->at(prev_size + i), v_predicates,
                               0, complete_gcrs);
    }
  }
  return true;
}

bool GCRFactory::MergeAndCompleteGCRs(const GCR& gcr, PathRule* path_rule,
                                      size_t max_path_num,
                                      std::vector<GCR>* complete_gcrs) {
  bool merge_status = false;

  // VertexLabel path_rule_label =
  // std::get<0>(path_rule->get_path_pattern()[0]); VertexLabel left_star_label
  // =
  //     std::get<0>(gcr.get_left_star()[0]->get_path_pattern()[0]);
  // VertexLabel right_star_label =
  //     std::get<0>(gcr.get_right_star()[0]->get_path_pattern()[0]);

  // // Check whether the path rule can be added to the left star.
  // if (path_rule_label == left_star_label &&
  //     gcr.get_left_star().size() < max_path_num) {
  //   GCR new_gcr = gcr;
  //   new_gcr.AddPathRuleToLeftStar(path_rule);
  //   merge_status = InitializeGCRs(new_gcr, true, complete_gcrs);
  // }

  // // Check whether the path rule can be added to the right star.
  // if (path_rule_label == right_star_label &&
  //     gcr.get_right_star().size() < max_path_num) {
  //   GCR new_gcr = gcr;
  //   new_gcr.AddPathRuleToRigthStar(path_rule);
  //   merge_status = InitializeGCRs(new_gcr, false, complete_gcrs);
  // }

  return merge_status;
}

bool GCRFactory::ConcretizeVariablePredicates(
    const GCR& gcr, const VariablePredicate& variable_predicate,
    bool added_to_left_star, bool consider_consequence,
    std::vector<ConcreteVariablePredicate>* predicates) const {
  // VertexLabel lhs_label = variable_predicate.get_lhs_label();
  // VertexLabel rhs_label = variable_predicate.get_rhs_label();
  // // TODO: in this version, only variable predicates with lsh and rhs in
  // // different stars are considered. Consider cases within the same star for
  // // variable predicates in precondition in the future.
  // size_t lhs_start_index = 0;
  // size_t rhs_start_index = 0;
  // if (added_to_left_star) {
  //   lhs_start_index = gcr.get_left_star().size() - 1;
  // } else {
  //   rhs_start_index = gcr.get_right_star().size() - 1;
  // }

  // for (size_t i = lhs_start_index; i < gcr.get_left_star().size(); i++) {
  //   for (size_t j = rhs_start_index; j < gcr.get_right_star().size(); j++) {
  //     // For each pair of paths.
  //     for (size_t k = 0; k <
  //     gcr.get_left_star()[i]->get_path_pattern().size();
  //          k++) {
  //       if (std::get<0>(gcr.get_left_star()[i]->get_path_pattern()[k]) !=
  //           lhs_label)
  //         continue;
  //       for (size_t l = 0;
  //            l < gcr.get_right_star()[j]->get_path_pattern().size(); l++) {
  //         if (std::get<0>(gcr.get_right_star()[j]->get_path_pattern()[l]) !=
  //             rhs_label)
  //           continue;
  //         const auto& concrete_vpredicate =
  //             gcr.ConcretizeVariablePredicate(variable_predicate, i, k, j,
  //             l);
  //         bool compatible_status =
  //             gcr.IsCompatibleWith(concrete_vpredicate,
  //             consider_consequence);
  //         if (compatible_status) {
  //           predicates->push_back(concrete_vpredicate);
  //         }
  //       }
  //     }
  //   }
  // }
  // return predicates->size() > 0;
  return false;
}

void GCRFactory::ExtendVariablePredicates(
    const GCR& gcr, std::vector<ConcreteVariablePredicate>& predicates,
    size_t start_id, std::vector<GCR>* gcrs) const {
  if (gcr.CountPreconditions() >= max_predicate_num_) return;

  GCR new_gcr = gcr;
  for (size_t i = start_id; i < predicates.size(); i++) {
    if (!new_gcr.IsCompatibleWith(predicates[i], true)) continue;
    new_gcr.AddVariablePredicateToBack(predicates[i]);
    gcrs->push_back(new_gcr);
    ExtendVariablePredicates(new_gcr, predicates, i + 1, gcrs);
    bool pop_status = new_gcr.PopVariablePredicateFromBack();
    if (!pop_status) {
      LOG_FATAL("GCRFactory::ExtendVariablePredicates: pop failed.");
    }
  }
}

}  // namespace sics::graph::miniclean::data_structures::gcr