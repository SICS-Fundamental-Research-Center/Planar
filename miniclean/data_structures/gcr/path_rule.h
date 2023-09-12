#ifndef MINICLEAN_DATA_STRUCTURES_GCR_PATH_RULE_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_PATH_RULE_H_

#include <list>

#include "miniclean/common/types.h"
#include "miniclean/data_structures/gcr/refactor_predicate.h"

namespace sics::graph::miniclean::data_structures::gcr {

// Path rule contains:
//   - a path pattern
//   - several constant predicates on its non-center vertices.
// It's used to compose with a GCR.
class PathRule {
 private:
  using PathPatternID = sics::graph::miniclean::common::PathPatternID;
  using ConstantPredicate =
      sics::graph::miniclean::data_structures::gcr::refactor::ConstantPredicate;

 public:
 private:
  PathPatternID path_pattern_id_;

  // The first element is the index of vertex in the pattern;
  // The second element the constant predicate.
  std::vector<std::pair<uint8_t, ConstantPredicate>> constant_predicates_;
};

}  // namespace sics::graph::miniclean::data_structures::gcr

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_PATH_RULE_H_