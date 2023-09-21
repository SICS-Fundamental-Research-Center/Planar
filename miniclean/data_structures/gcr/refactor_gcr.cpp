#include "miniclean/data_structures/gcr/refactor_gcr.h"

#include "miniclean/data_structures/gcr/refactor_predicate.h"

namespace sics::graph::miniclean::data_structures::gcr::refactor {

std::pair<size_t, size_t> GCR::ComputeMatchAndSupport() const {
  // TODO: Implement this.
  return std::make_pair<size_t, size_t>(0, 0);
}

size_t GCR::CountPreconditions() const {
  size_t count = 0;
  for (const auto& left_path : left_star_) {
    count += left_path->get_constant_predicates().size();
  }
  for (const auto& right_path : right_star_) {
    count += right_path->get_constant_predicates().size();
  }
  count += variable_predicates_.size();
  return count;
}

bool GCR::IsCompatibleWith(const ConcreteVariablePredicate& variable_predicate,
                           bool consider_consequence) const {
  uint8_t left_path_index = variable_predicate.get_left_path_index();
  uint8_t left_vertex_index = variable_predicate.get_left_vertex_index();
  uint8_t right_path_index = variable_predicate.get_right_path_index();
  uint8_t right_vertex_index = variable_predicate.get_right_vertex_index();

  // Check Variable Predicates.
  for (const auto& predicate : variable_predicates_) {
    if (&predicate == nullptr) {
      LOG_INFO("GCR::IsCompatibleWith: predicate is nullptr.");
    }
    uint8_t predicate_left_path_index = predicate.get_left_path_index();
    uint8_t predicate_left_vertex_index = predicate.get_left_vertex_index();
    uint8_t predicate_right_path_index = predicate.get_right_path_index();
    uint8_t predicate_right_vertex_index = predicate.get_right_vertex_index();
    if ((left_path_index == predicate_left_path_index &&
         left_vertex_index == predicate_left_vertex_index) ||
        (right_path_index == predicate_right_path_index &&
         right_vertex_index == predicate_right_vertex_index)) {
      return false;
    }
  }

  if (consider_consequence) {
    // Check Consequence.
    uint8_t consequence_left_path_index = consequence_.get_left_path_index();
    uint8_t consequence_left_vertex_index =
        consequence_.get_left_vertex_index();
    uint8_t consequence_right_path_index = consequence_.get_right_path_index();
    uint8_t consequence_right_vertex_index =
        consequence_.get_right_vertex_index();
    if ((left_path_index == consequence_left_path_index &&
         left_vertex_index == consequence_left_vertex_index) ||
        (right_path_index == consequence_right_path_index &&
         right_vertex_index == consequence_right_vertex_index)) {
      return false;
    }
  }
  return true;
}

}  // namespace sics::graph::miniclean::data_structures::gcr::refactor