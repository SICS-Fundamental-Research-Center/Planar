#include "miniclean/data_structures/gcr/path_rule.h"

#include <algorithm>

#include "miniclean/common/types.h"

namespace sics::graph::miniclean::data_structures::gcr {

using VertexID = sics::graph::miniclean::common::VertexID;

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

void StarRule::InitializeStarRule() { valid_vertices_ = ComputeValidCenters(); }

size_t StarRule::ComputeInitSupport() const {
  std::vector<VertexID> valid_centers = ComputeValidCenters();
  return valid_centers.size();
}

std::vector<VertexID> StarRule::ComputeValidCenters() const {
  std::vector<VertexID> results;
  std::vector<VertexID> valid_centers;
  bool has_intersected = false;
  for (const auto& predicate : constant_predicates_) {
    if (predicate.get_operator_type() != refactor::OperatorType::kEq) {
      LOG_FATAL("Only support constant predicates with operator type kEq.");
    }
    auto vlabel = predicate.get_vertex_label();
    auto vattr_id = predicate.get_vertex_attribute_id();
    auto vattr_value = predicate.get_constant_value();
    const auto& attr_bucket_by_vlabel =
        index_collection_->GetAttributeBucketByVertexLabel(vlabel);
    // This vector has been sorted.
    std::vector<VertexID> valid_vertices =
        attr_bucket_by_vlabel.at(vattr_id).at(vattr_value);
    if (has_intersected) {
      std::set_intersection(valid_centers.begin(), valid_centers.end(),
                            valid_vertices.begin(), valid_vertices.end(),
                            std::back_inserter(results));
      valid_centers = std::move(results);
    } else {
      valid_centers = std::move(valid_vertices);
    }
  }
}

}  // namespace sics::graph::miniclean::data_structures::gcr