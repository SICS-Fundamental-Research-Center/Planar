#ifndef MINICLEAN_DATA_STRUCTURES_GCR_PATH_RULE_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_PATH_RULE_H_

#include <map>
#include <unordered_set>

#include "miniclean/common/types.h"
#include "miniclean/components/preprocessor/index_collection.h"
#include "miniclean/data_structures/gcr/predicate.h"
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
      sics::graph::miniclean::data_structures::gcr::ConstantPredicate;
  using MiniCleanCSRGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
  using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;
  using VertexAttributeValue =
      sics::graph::miniclean::common::VertexAttributeValue;
  using IndexCollection =
      sics::graph::miniclean::components::preprocessor::IndexCollection;
  using PathInstance = std::vector<VertexID>;
  using PathInstanceBucket = std::vector<PathInstance>;

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

  // TODO: replace the std::pair with `ConcreteConstantPredicate`.
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

  // This function returns the index of path instance if matching successfully.
  // Otherwise, it would return the number of path instances.
  size_t GetPathRuleIndex(const MiniCleanCSRGraph& graph,
                          const PathInstanceBucket& path_instance_bucket,
                          const std::vector<size_t>& visited,
                          size_t start_pos) const;

  std::string GetInfoString(
      const std::vector<PathPattern>& path_patterns) const;

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
      sics::graph::miniclean::data_structures::gcr::ConstantPredicate;
  using VertexAttributeValue =
      sics::graph::miniclean::common::VertexAttributeValue;
  using IndexCollection =
      sics::graph::miniclean::components::preprocessor::IndexCollection;
  using MiniCleanCSRGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using PathPattern = sics::graph::miniclean::common::PathPattern;
  using PathPatternID = sics::graph::miniclean::common::PathPatternID;
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;

 public:
  StarRule() = default;

  StarRule(VertexLabel center_label, const IndexCollection& index_collection)
      : constant_predicate_count_(0),
        center_label_(center_label),
        index_collection_(index_collection) {}

  StarRule(VertexLabel center_label, ConstantPredicate constant_predicate,
           VertexAttributeValue attribute_value,
           const IndexCollection& index_collection)
      : constant_predicate_count_(1),
        center_label_(center_label),
        index_collection_(index_collection) {
    constant_predicate.set_constant_value(attribute_value);
    constant_predicates_.emplace_back(constant_predicate);
  }

  // StarRule(const StarRule& other) : index_collection_(other.index_collection_) {
  //   constant_predicate_count_ = other.constant_predicate_count_;
  //   center_label_ = other.center_label_;
  //   constant_predicates_ = other.constant_predicates_;
  //   path_rules_ = other.path_rules_;
  //   valid_vertex_buckets_ = other.valid_vertex_buckets_;
  // }

  VertexLabel get_center_label() const { return center_label_; }

  const std::vector<ConstantPredicate>& get_constant_predicates() const {
    return constant_predicates_;
  }

  size_t get_constant_predicate_count() const {
    return constant_predicate_count_;
  }

  const std::vector<PathRule>& get_path_rules() const { return path_rules_; }

  const IndexCollection& get_index_collection() const {
    return index_collection_;
  }

  const std::vector<std::unordered_set<VertexID>>& get_valid_vertex_bucket()
      const {
    return valid_vertex_buckets_;
  }

  std::vector<std::unordered_set<VertexID>>* get_mutable_valid_vertex_bucket() {
    return &valid_vertex_buckets_;
  }

  void AddConstantPredicateToBack(const ConstantPredicate& constant_predicate) {
    constant_predicates_.emplace_back(constant_predicate);
  }

  void UpdateValidVertexBucket(
      std::vector<std::unordered_set<VertexID>>&& new_valid_vertex_bucket) {
    valid_vertex_buckets_ = std::move(new_valid_vertex_bucket);
  }

  void UpdateValidVertexBucket(const MiniCleanCSRGraph& graph);

  void AddPathRule(const PathRule& path_rule) {
    path_rules_.emplace_back(path_rule);
    constant_predicate_count_ += path_rule.get_constant_predicates().size();
  }

  void RemoveLastPathRule() {
    if (path_rules_.empty()) LOG_FATAL("Path rule is empty.");
    path_rules_.pop_back();
  }

  void ComposeWith(const StarRule& other);

  void InitializeStarRule();
  size_t ComputeInitSupport();
  void SetIntersection(std::unordered_set<VertexID>* base_set,
                       std::unordered_set<VertexID>* comp_set,
                       std::unordered_set<VertexID>* diff_set);

  std::string GetInfoString(
      const std::vector<PathPattern>& path_patterns) const;

 private:
  std::unordered_set<VertexID> ComputeValidCenters();
  bool TestStarRule(const MiniCleanCSRGraph& graph, VertexID center_id) const;

  size_t constant_predicate_count_;
  VertexLabel center_label_;
  std::vector<ConstantPredicate> constant_predicates_;
  std::vector<PathRule> path_rules_;
  const IndexCollection& index_collection_;

  std::vector<std::unordered_set<VertexID>> valid_vertex_buckets_;
};

}  // namespace sics::graph::miniclean::data_structures::gcr

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_PATH_RULE_H_