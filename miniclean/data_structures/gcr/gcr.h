#ifndef MINICLEAN_DATA_STRUCTURES_GCR_GCR_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_GCR_H_

#include <list>
#include <vector>

#include "miniclean/common/config.h"
#include "miniclean/data_structures/gcr/path_rule.h"
#include "miniclean/data_structures/gcr/predicate.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::data_structures::gcr {

struct GCRHorizontalExtension {
  GCRHorizontalExtension(
      const ConcreteVariablePredicate& consequence,
      const std::vector<ConcreteVariablePredicate>& variable_predicates)
      : consequence(consequence), variable_predicates(variable_predicates) {}
  ConcreteVariablePredicate consequence;
  std::vector<ConcreteVariablePredicate> variable_predicates;
};

struct GCRVerticalExtension {
  GCRVerticalExtension(bool extend_to_left, const PathRule& path_rule)
      : extend_to_left(extend_to_left), path_rule(path_rule) {}
  bool extend_to_left;
  PathRule path_rule;
};

struct BucketID {
  BucketID() = default;
  BucketID(sics::graph::miniclean::common::VertexLabel left_label,
           sics::graph::miniclean::common::VertexAttributeID left_attribute_id,
           sics::graph::miniclean::common::VertexLabel right_label,
           sics::graph::miniclean::common::VertexAttributeID right_attribute_id)
      : left_label(left_label),
        left_attribute_id(left_attribute_id),
        right_label(right_label),
        right_attribute_id(right_attribute_id) {}
  sics::graph::miniclean::common::VertexLabel left_label;
  sics::graph::miniclean::common::VertexAttributeID left_attribute_id;
  sics::graph::miniclean::common::VertexLabel right_label;
  sics::graph::miniclean::common::VertexAttributeID right_attribute_id;
};

class GCR {
 private:
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
  using PathRule = sics::graph::miniclean::data_structures::gcr::PathRule;
  using StarRule = sics::graph::miniclean::data_structures::gcr::StarRule;
  using VariablePredicate =
      sics::graph::miniclean::data_structures::gcr::VariablePredicate;
  using ConcreteVariablePredicate =
      sics::graph::miniclean::data_structures::gcr::ConcreteVariablePredicate;
  using MiniCleanCSRGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using PathPattern = sics::graph::miniclean::common::PathPattern;
  using PathInstance = std::vector<VertexID>;
  using PathInstanceBucket = std::vector<PathInstance>;
  using Configurations = sics::graph::miniclean::common::Configurations;

 public:
  GCR(const StarRule& left_star, const StarRule& right_star)
      : left_star_(left_star), right_star_(right_star), support_(0), match_(0) {
    left_star_.InitializeStarRule();
    right_star_.InitializeStarRule();
    bucket_id_ = BucketID(MAX_VERTEX_LABEL, MAX_VERTEX_ATTRIBUTE_ID,
                          MAX_VERTEX_LABEL, MAX_VERTEX_ATTRIBUTE_ID);
    // First item is <vertical extension id, vertical extension num>.
    // Their value is (0, 1) since we start with two isolated star centers (one
    // vertical extension).
    mining_progress_log_.reserve((Configurations::Get()->max_path_num_ + 1) *
                                 2);
    mining_progress_log_.emplace_back(0, 1);
  }

  void AddVariablePredicateToBack(
      const ConcreteVariablePredicate& variable_predicate) {
    variable_predicates_.push_back(variable_predicate);
  }

  bool PopVariablePredicateFromBack() {
    if (!variable_predicates_.empty()) {
      variable_predicates_.pop_back();
      return true;
    }
    return false;
  }

  void AddPathRuleToLeftStar(const PathRule& path_rule) {
    left_star_.AddPathRule(path_rule);
  }

  void AddPathRuleToRigthStar(const PathRule& path_rule) {
    right_star_.AddPathRule(path_rule);
  }

  void set_consequence(const ConcreteVariablePredicate& consequence) {
    consequence_ = consequence;
  }

  const StarRule& get_left_star() const { return left_star_; }
  const StarRule& get_right_star() const { return right_star_; }

  const std::vector<ConcreteVariablePredicate>& get_variable_predicates()
      const {
    return variable_predicates_;
  }

  const ConcreteVariablePredicate& get_consequence() const {
    return consequence_;
  }

  size_t get_constant_predicate_count() const {
    size_t left_count = left_star_.get_predicate_count();
    size_t right_count = right_star_.get_predicate_count();
    return left_count + right_count;
  }

  size_t get_variable_predicate_count() const {
    return variable_predicates_.size();
  }

  void Recover(bool horizontal_recover);
  void ExtendVertically(const GCRVerticalExtension& vertical_extension,
                        const MiniCleanCSRGraph& graph,
                        size_t vertical_extension_id,
                        size_t vertical_extension_num);
  void ExtendHorizontally(const GCRHorizontalExtension& horizontal_extension,
                          const MiniCleanCSRGraph& graph,
                          size_t horizontal_extension_id,
                          size_t horizontal_extension_num);
  // TODO: this function should be more rigorous.
  std::pair<size_t, size_t> ComputeMatchAndSupport(
      const MiniCleanCSRGraph& graph);

  bool IsCompatibleWith(const ConcreteVariablePredicate& variable_predicate,
                        bool consider_consequence) const;

  std::string GetInfoString(const std::vector<PathPattern>& path_patterns,
                            size_t match, size_t support,
                            float confidence) const;

 private:
  void Backup(const MiniCleanCSRGraph& graph, bool added_to_left_star);
  void InitializeBuckets(const MiniCleanCSRGraph& graph,
                         const ConcreteVariablePredicate& c_variable_predicate);
  bool TestVariablePredicate(
      const MiniCleanCSRGraph& graph,
      const ConcreteVariablePredicate& variable_predicate, VertexID left_vid,
      VertexID right_vid) const;

  StarRule left_star_;
  StarRule right_star_;

  std::vector<ConcreteVariablePredicate> variable_predicates_;
  ConcreteVariablePredicate consequence_;

  // item value is the number of variable predicates that added to the GCR.
  std::list<size_t> horizontal_extension_log_;
  // `true` if added to left star, `false` if added to right star.
  std::list<bool> vertical_extension_log_;

  std::vector<std::pair<size_t, size_t>> mining_progress_log_;

  BucketID bucket_id_;

  size_t support_;
  size_t match_;
};

}  // namespace sics::graph::miniclean::data_structures::gcr

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_GCR_H_