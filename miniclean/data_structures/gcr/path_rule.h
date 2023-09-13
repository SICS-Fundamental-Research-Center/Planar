#ifndef MINICLEAN_DATA_STRUCTURES_GCR_PATH_RULE_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_PATH_RULE_H_

#include <list>

#include "miniclean/common/types.h"
#include "miniclean/components/preprocessor/star_bitmap.h"
#include "miniclean/data_structures/gcr/refactor_predicate.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::data_structures::gcr {

// Path rule contains:
//   - a path pattern
//   - several constant predicates on its non-center vertices.
// It's used to compose with a GCR.
class PathRule {
 private:
  using PathPatternID = sics::graph::miniclean::common::PathPatternID;
  using PathPattern = sics::graph::miniclean::common::PathPattern;
  using ConstantPredicate =
      sics::graph::miniclean::data_structures::gcr::refactor::ConstantPredicate;
  using StarBitmap =
      sics::graph::miniclean::components::preprocessor::StarBitmap;
  using MiniCleanCSRGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using VertexID = sics::graph::miniclean::common::VertexID;

 public:
  PathRule(PathPattern path_pattern, size_t map_size)
      : path_pattern_(path_pattern), star_bitmap_(map_size) {}

  void AddConstantPredicate(uint8_t vertex_index,
                            ConstantPredicate constant_predicate) {
    constant_predicates_.emplace_back(vertex_index, constant_predicate);
  }
  void PopConstantPredicate() {
    if (!constant_predicates_.empty()) {
      constant_predicates_.pop_back();
    }
  }

  void InitBitmap(std::vector<std::vector<VertexID>> path_instance,
                  MiniCleanCSRGraph* graph);

  const PathPattern& get_path_pattern() const { return path_pattern_; }

  const std::vector<std::pair<uint8_t, ConstantPredicate>>&
  get_constant_predicates() const {
    return constant_predicates_;
  }

 private:
  PathPattern path_pattern_;

  // The first element is the index of vertex in the pattern;
  // The second element the constant predicate.
  std::vector<std::pair<uint8_t, ConstantPredicate>> constant_predicates_;

  // The size of the star bitmap is the number of vertices in the graph.
  // The bit at position `i` is set to 1 if the vertex `i` can be a valid center
  // vertex of this path.
  StarBitmap star_bitmap_;
};

}  // namespace sics::graph::miniclean::data_structures::gcr

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_PATH_RULE_H_