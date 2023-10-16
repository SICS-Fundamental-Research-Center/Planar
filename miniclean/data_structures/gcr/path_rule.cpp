#include "miniclean/data_structures/gcr/path_rule.h"

#include <algorithm>

#include "miniclean/common/types.h"

namespace sics::graph::miniclean::data_structures::gcr {

PathRule::PathRule(PathPatternID path_pattern_id, size_t vertex_pos,
                   ConstantPredicate constant_predicate,
                   VertexAttributeValue attribute_value)
    : path_pattern_id_(path_pattern_id) {
  constant_predicate.set_constant_value(attribute_value);
  constant_predicates_.emplace_back(vertex_pos, std::move(constant_predicate));
}

void PathRule::ComposeWith(const PathRule& other) {
  // Check whether the two path rules have the same path pattern.
  if (path_pattern_id_ != other.path_pattern_id_) {
    LOG_FATAL("Path rules with different path patterns cannot be composed: ",
              path_pattern_id_, " vs. ", other.path_pattern_id_);
  }
  // Compose path rules.
  auto other_constant_predicates = other.get_constant_predicates();
  for (const auto& other_constant_predicate : other_constant_predicates) {
    constant_predicates_.emplace_back(other_constant_predicate);
  }
}

size_t PathRule::ComputeInitSupport() {
  LOG_FATAL("Not implemented yet.");
  return 0;
}

void StarRule::ComposeWith(const StarRule& other) {
  // Check whether two star rules have th same path pattern.
  if (center_label_ != other.center_label_) {
    LOG_FATAL("Star rules with different center label cannot be composed: ",
              center_label_, " vs. ", other.center_label_);
  }
  // Only support composing centers.
  if (path_rules_.size() != 0 || other.path_rules_.size() != 0) {
    LOG_FATAL("Do not support composing star rules with non-empty path rules.");
  }
  // Compose star rules.
  auto other_constant_predicates = other.get_constant_predicates();
  for (const auto& other_constant_predicate : other_constant_predicates) {
    constant_predicates_.emplace_back(other_constant_predicate);
  }
  predicate_count_ += other.predicate_count_;
}

void StarRule::InitializeStarRule() { ComputeValidCenters(&valid_vertices_); }

size_t StarRule::ComputeInitSupport() {
  std::unordered_set<VertexID> valid_centers;
  ComputeValidCenters(&valid_centers);
  return valid_centers.size();
}

void StarRule::ComputeValidCenters(
    std::unordered_set<VertexID>* valid_centers) {
  // Get the valid centers from the first constant predicate.
  auto vlabel0 = constant_predicates_[0].get_vertex_label();
  auto vattr_id0 = constant_predicates_[0].get_vertex_attribute_id();
  auto vattr_value0 = constant_predicates_[0].get_constant_value();
  const auto& attr_bucket_by_vlabel0 =
      index_collection_->GetAttributeBucketByVertexLabel(vlabel0);
  (*valid_centers) = attr_bucket_by_vlabel0.at(vattr_id0).at(vattr_value0);
  // Compute the intersection.
  for (size_t i = 1; i < constant_predicates_.size(); i++) {
    if (constant_predicates_[i].get_operator_type() != refactor::OperatorType::kEq) {
      LOG_FATAL("Only support constant predicates with operator type kEq.");
    }
    auto vlabel = constant_predicates_[i].get_vertex_label();
    auto vattr_id = constant_predicates_[i].get_vertex_attribute_id();
    auto vattr_value = constant_predicates_[i].get_constant_value();
    const auto& attr_bucket_by_vlabel =
        index_collection_->GetAttributeBucketByVertexLabel(vlabel);
    std::unordered_set<VertexID> valid_vertices =
        attr_bucket_by_vlabel.at(vattr_id).at(vattr_value);
    std::unordered_set<VertexID> diff;
    SetIntersection(valid_centers, &valid_vertices, &diff);
  }
}

void StarRule::SetIntersection(std::unordered_set<VertexID>* base_set,
                               std::unordered_set<VertexID>* comp_set,
                               std::unordered_set<VertexID>* diff_set) {
  for (auto it = (*base_set).begin(); it != (*base_set).end();) {
    if (comp_set->count(*it) > 0) {
      ++it;
    } else {
      diff_set->emplace(*it);
      it = (*base_set).erase(it);
    }
  }
}

}  // namespace sics::graph::miniclean::data_structures::gcr