#ifndef MINICLEAN_DATA_STRUCTURES_GCR_PATH_RULE_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_PATH_RULE_H_

#include <unordered_set>

#include "miniclean/common/types.h"
#include "miniclean/components/preprocessor/index_collection.h"
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
  using MiniCleanCSRGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexAttributeValue =
      sics::graph::miniclean::common::VertexAttributeValue;
  using IndexCollection =
      sics::graph::miniclean::components::preprocessor::IndexCollection;

 public:
  PathRule() = default;
  PathRule(PathPatternID path_pattern_id) : path_pattern_id_(path_pattern_id) {}
  PathRule(const PathRule& other)
      : path_pattern_id_(other.path_pattern_id_),
        constant_predicates_(other.constant_predicates_) {}
  PathRule(PathPatternID path_pattern_id, size_t vertex_pos,
           ConstantPredicate constant_predicate,
           VertexAttributeValue attribute_value);

  size_t get_path_pattern_id() const { return path_pattern_id_; }

  const std::vector<std::pair<uint8_t, ConstantPredicate>>&
  get_constant_predicates() const {
    return constant_predicates_;
  }

  void AddConstantPredicate(uint8_t vertex_index,
                            const ConstantPredicate& constant_predicate) {
    constant_predicates_.emplace_back(vertex_index, constant_predicate);
  }

  size_t ComputeInitSupport();

  bool PopConstantPredicate() {
    if (!constant_predicates_.empty()) {
      constant_predicates_.pop_back();
      return true;
    }
    return false;
  }

  // Compose with another path rule with the same path pattern.
  void ComposeWith(const PathRule& other);

 private:
  size_t path_pattern_id_;
  // The first element is the index of vertex in the pattern;
  // The second element the constant predicate.
  std::vector<std::pair<uint8_t, ConstantPredicate>> constant_predicates_;
};

class StarRule {
 private:
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using ConstantPredicate =
      sics::graph::miniclean::data_structures::gcr::refactor::ConstantPredicate;
  using VertexAttributeValue =
      sics::graph::miniclean::common::VertexAttributeValue;
  using IndexCollection =
      sics::graph::miniclean::components::preprocessor::IndexCollection;

 public:
  StarRule(VertexLabel center_label, IndexCollection* index_collection)
      : predicate_count_(0),
        center_label_(center_label),
        index_collection_(index_collection) {}
  StarRule(VertexLabel center_label, ConstantPredicate constant_predicate,
           VertexAttributeValue attribute_value,
           IndexCollection* index_collection)
      : predicate_count_(1),
        center_label_(center_label),
        index_collection_(index_collection) {
    constant_predicate.set_constant_value(attribute_value);
    constant_predicates_.emplace_back(constant_predicate);
  }

  VertexLabel get_center_label() const { return center_label_; }

  const std::vector<ConstantPredicate>& get_constant_predicates() const {
    return constant_predicates_;
  }

  size_t get_predicate_count() const { return predicate_count_; }

  const std::vector<PathRule>& get_path_rules() const { return path_rules_; }

  // TODO: use reference instead.
  IndexCollection* get_index_collection() const {
    return index_collection_;
  }

  const std::unordered_set<VertexID>& get_valid_vertices() const {
    return valid_vertices_;
  }

  const std::vector<std::unordered_set<VertexID>>& get_bucket() const {
    return bucket_;
  }

  void ReserveBucket(size_t size) { bucket_.reserve(size); }
  void AddVertexToBucket(size_t bucket_index, VertexID vertex_id) {
    bucket_[bucket_index].emplace(vertex_id);
  }

  void AddPathRule(const PathRule& path_rule) {
    path_rules_.emplace_back(path_rule);
    predicate_count_ += path_rule.get_constant_predicates().size();
  }

  void ComposeWith(const StarRule& other);

  void InitializeStarRule();
  size_t ComputeInitSupport();
  void SetIntersection(std::unordered_set<VertexID>* base_set,
                       std::unordered_set<VertexID>* comp_set,
                       std::unordered_set<VertexID>* diff_set);

  void Backup();
  void Recover();

 private:
  std::unordered_set<VertexID> ComputeValidCenters();

  size_t predicate_count_;
  VertexLabel center_label_;
  std::vector<ConstantPredicate> constant_predicates_;
  std::vector<PathRule> path_rules_;
  IndexCollection* index_collection_;

  std::unordered_set<VertexID> valid_vertices_;
  std::unordered_set<VertexID> valid_vertices_diff_;
  std::vector<std::unordered_set<VertexID>> bucket_;
  std::vector<std::unordered_set<VertexID>> bucket_diff_;
};

}  // namespace sics::graph::miniclean::data_structures::gcr

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_PATH_RULE_H_