#include "miniclean/data_structures/gcr/gcr.h"

#include "core/util/logging.h"

namespace sics::graph::miniclean::data_structures::gcr {

using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
using VertexAttributeValue =
    sics::graph::miniclean::common::VertexAttributeValue;
using PathPatternID = sics::graph::miniclean::common::PathPatternID;

void GCR::Init(MiniCleanCSRGraph* graph,
               std::vector<std::vector<std::vector<VertexID>>> path_instances) {
  // Retrieve path instances of left and right path pattern.
  std::vector<std::vector<VertexID>> left_path_instances =
      path_instances[dual_pattern_.first.front()];
  std::vector<std::vector<VertexID>> right_path_instances =
      path_instances[dual_pattern_.second.front()];

  size_t local_support = 0;
  size_t local_match = 0;

  for (PathInstanceID i = 0; i < left_path_instances.size(); i++) {
    for (PathInstanceID j = 0; j < right_path_instances.size(); j++) {
      bool is_precondition_satisfied = true;
      bool is_consequence_satisfied = true;
      // Check preconditions.
      for (const auto& precondition : preconditions_) {
        if (precondition->get_predicate_type() == kConstantPredicate) {
          ConstantPredicate* constant_precondition =
              dynamic_cast<ConstantPredicate*>(precondition);
          if (constant_precondition->is_in_lhs_pattern()) {
            is_precondition_satisfied = constant_precondition->ConstantCompare(
                graph, left_path_instances[i]);
          } else {
            is_precondition_satisfied = constant_precondition->ConstantCompare(
                graph, right_path_instances[j]);
          }
          if (!is_precondition_satisfied) {
            break;
          }
        } else if (precondition->get_predicate_type() == kVariablePredicate) {
          VariablePredicate* variable_precondition =
              dynamic_cast<VariablePredicate*>(precondition);

          is_precondition_satisfied = variable_precondition->VariableCompare(
              graph, left_path_instances[i], right_path_instances[j]);

          if (!is_precondition_satisfied) {
            break;
          }
        } else {
          LOG_FATAL("Unsupported predicate type.");
        }
      }
      if (!is_precondition_satisfied) {
        continue;
      }
      local_match++;
      // Check consequences.
      if (consequence_->get_predicate_type() == kConstantPredicate) {
        ConstantPredicate* constant_consequence =
            dynamic_cast<ConstantPredicate*>(consequence_);
        if (constant_consequence->is_in_lhs_pattern()) {
          is_consequence_satisfied = constant_consequence->ConstantCompare(
              graph, left_path_instances[i]);
        } else {
          is_consequence_satisfied = constant_consequence->ConstantCompare(
              graph, right_path_instances[j]);
        }
      } else if (consequence_->get_predicate_type() == kVariablePredicate) {
        VariablePredicate* variable_consequence =
            dynamic_cast<VariablePredicate*>(consequence_);
        is_consequence_satisfied = variable_consequence->VariableCompare(
            graph, left_path_instances[i], right_path_instances[j]);
      } else {
        LOG_FATAL("Unsupported predicate type.");
      }

      if (is_consequence_satisfied) {
        local_support++;
        AddGCRInstanceToBack(std::make_pair(std::list<PathInstanceID>{i},
                                            std::list<PathInstanceID>{j}));
      }
    }
  }
  set_local_support(local_support);
  set_local_match(local_match);
}

}  // namespace sics::graph::miniclean::data_structures::gcr