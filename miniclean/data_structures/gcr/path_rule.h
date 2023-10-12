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
  using VertexAttributeValue =
      sics::graph::miniclean::common::VertexAttributeValue;

 public:
  PathRule() = default;
  PathRule(PathPatternID path_pattern_id, size_t vertex_pos,
           ConstantPredicate constant_predicate,
           VertexAttributeValue attribute_value)
      : path_pattern_id_(path_pattern_id) {
    constant_predicate.set_constant_value(attribute_value);
    constant_predicates_.emplace_back(vertex_pos, constant_predicate);
  }
  PathRule(PathPattern path_pattern, size_t path_pattern_id, size_t map_size)
      : path_pattern_(path_pattern),
        path_pattern_id_(path_pattern_id),
        star_bitmap_(map_size) {}
  PathRule(const PathRule& other)
      : path_pattern_(other.path_pattern_),
        path_pattern_id_(other.path_pattern_id_),
        star_bitmap_(other.star_bitmap_),
        constant_predicates_(other.constant_predicates_) {}

  void AddConstantPredicate(uint8_t vertex_index,
                            const ConstantPredicate& constant_predicate) {
    constant_predicates_.emplace_back(vertex_index, constant_predicate);
  }
  bool PopConstantPredicate() {
    if (!constant_predicates_.empty()) {
      constant_predicates_.pop_back();
      return true;
    }
    return false;
  }

  size_t CountOneBits() { return star_bitmap_.CountOneBits(); }

  void InitBitmap(std::vector<std::vector<VertexID>> path_instance,
                  MiniCleanCSRGraph* graph);

  // Compose with another path rule with the same path pattern.
  bool ComposeWith(PathRule& other, size_t max_pred_num);

  const PathPattern& get_path_pattern() const { return path_pattern_; }

  size_t get_path_pattern_id() const { return path_pattern_id_; }

  const std::vector<std::pair<uint8_t, ConstantPredicate>>&
  get_constant_predicates() const {
    return constant_predicates_;
  }

  const StarBitmap& get_star_bitmap() const { return star_bitmap_; }

 private:
  PathPattern path_pattern_;
  size_t path_pattern_id_;

  // The first element is the index of vertex in the pattern;
  // The second element the constant predicate.
  std::vector<std::pair<uint8_t, ConstantPredicate>> constant_predicates_;

  // The size of the star bitmap is the number of vertices in the graph.
  // The bit at position `i` is set to 1 if the vertex `i` can be a valid center
  // vertex of this path.
  StarBitmap star_bitmap_;
};

class StarRule {
 private:
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;
  using ConstantPredicate =
      sics::graph::miniclean::data_structures::gcr::refactor::ConstantPredicate;
  using VertexAttributeValue =
      sics::graph::miniclean::common::VertexAttributeValue;

 public:
  StarRule(VertexLabel center_label)
      : predicate_count_(0), center_label_(center_label) {}
  StarRule(VertexLabel center_label, ConstantPredicate constant_predicate,
           VertexAttributeValue attribute_value)
      : predicate_count_(1), center_label_(center_label) {
    constant_predicate.set_constant_value(attribute_value);
    constant_predicates_.emplace_back(constant_predicate);
  }

  void AddConstantPredicateToCenter(
      const ConstantPredicate& constant_predicate) {
    constant_predicates_.emplace_back(constant_predicate);
    predicate_count_ += 1;
  }
  void AddPathRule(PathRule path_rule) {
    path_rules_.push_back(path_rule);
    predicate_count_ += path_rule.get_constant_predicates().size();
  }

 private:
  size_t predicate_count_;
  VertexLabel center_label_;
  std::vector<ConstantPredicate> constant_predicates_;
  std::vector<PathRule> path_rules_;
};

}  // namespace sics::graph::miniclean::data_structures::gcr

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_PATH_RULE_H_