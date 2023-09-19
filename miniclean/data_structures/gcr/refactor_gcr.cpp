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

}  // namespace sics::graph::miniclean::data_structures::gcr::refactor