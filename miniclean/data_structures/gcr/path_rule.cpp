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
  const auto& other_constant_predicates = other.get_constant_predicates();
  for (const auto& other_constant_predicate : other_constant_predicates) {
    constant_predicates_.emplace_back(other_constant_predicate);
  }
}

size_t PathRule::ComputeInitSupport() {
  LOG_FATAL("Not implemented yet.");
  return 0;
}

size_t PathRule::GetPathRuleIndex(
    const MiniCleanCSRGraph& graph,
    const PathInstanceBucket& path_instance_bucket,
    const std::vector<size_t>& visited, size_t start_pos) const {
  for (size_t i = start_pos; i < path_instance_bucket.size(); i++) {
    // Check whether i has been visited.
    for (const auto& visited_pos : visited) {
      if (visited_pos == i) continue;
    }
    auto path_instance = path_instance_bucket[i];
    for (const auto& const_pred : constant_predicates_) {
      auto vertex_id = path_instance[const_pred.first];
      auto vertex_value = graph.GetVertexAttributeValuesByLocalID(
          vertex_id)[const_pred.second.get_vertex_attribute_id()];
      if (vertex_value == const_pred.second.get_constant_value()) {
        return i;
      }
    }
  }
  return path_instance_bucket.size();
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
  const auto& other_constant_predicates = other.get_constant_predicates();
  for (const auto& other_constant_predicate : other_constant_predicates) {
    constant_predicates_.emplace_back(other_constant_predicate);
  }
  predicate_count_ += other.predicate_count_;
}

void StarRule::InitializeStarRule() {
  valid_vertex_buckets_.emplace_back(ComputeValidCenters());
}

size_t StarRule::ComputeInitSupport() {
  if (constant_predicates_.empty()) {
    std::pair<VertexID, VertexID> vertex_range =
        index_collection_.GetVertexRangeByLabelID(center_label_);
    return vertex_range.second - vertex_range.first;
  }
  size_t support = 0;
  auto vlabel0 = constant_predicates_[0].get_vertex_label();
  auto vattr_id0 = constant_predicates_[0].get_vertex_attribute_id();
  auto vattr_value0 = constant_predicates_[0].get_constant_value();
  const auto& attr_bucket_by_vlabel0 =
      index_collection_.GetAttributeBucketByVertexLabel(vlabel0);
  for (const auto& center :
       attr_bucket_by_vlabel0.at(vattr_id0).at(vattr_value0)) {
    bool valid = true;
    for (size_t i = 1; i < constant_predicates_.size(); i++) {
      if (constant_predicates_[i].get_operator_type() != OperatorType::kEq) {
        LOG_FATAL("Only support constant predicates with operator type kEq.");
      }
      auto vlabel = constant_predicates_[i].get_vertex_label();
      auto vattr_id = constant_predicates_[i].get_vertex_attribute_id();
      auto vattr_value = constant_predicates_[i].get_constant_value();
      const auto& attr_bucket_by_vlabel =
          index_collection_.GetAttributeBucketByVertexLabel(vlabel);
      if (attr_bucket_by_vlabel.at(vattr_id).at(vattr_value).count(center) ==
          0) {
        valid = false;
        break;
      }
    }
    if (valid) {
      support++;
    }
  }
  return support;
}

std::unordered_set<VertexID> StarRule::ComputeValidCenters() {
  std::unordered_set<VertexID> valid_centers;
  // Deal with empty constant predicates.
  if (constant_predicates_.empty()) {
    std::pair<VertexID, VertexID> vertex_range =
        index_collection_.GetVertexRangeByLabelID(center_label_);
    for (VertexID i = vertex_range.first; i < vertex_range.second; i++) {
      valid_centers.insert(i);
    }
    return valid_centers;
  }

  // Get the valid centers from the first constant predicate.
  auto vlabel0 = constant_predicates_[0].get_vertex_label();
  auto vattr_id0 = constant_predicates_[0].get_vertex_attribute_id();
  auto vattr_value0 = constant_predicates_[0].get_constant_value();
  const auto& attr_bucket_by_vlabel0 =
      index_collection_.GetAttributeBucketByVertexLabel(vlabel0);
  valid_centers = attr_bucket_by_vlabel0.at(vattr_id0).at(vattr_value0);
  // Compute the intersection.
  for (size_t i = 1; i < constant_predicates_.size(); i++) {
    if (constant_predicates_[i].get_operator_type() != OperatorType::kEq) {
      LOG_FATAL("Only support constant predicates with operator type kEq.");
    }
    auto vlabel = constant_predicates_[i].get_vertex_label();
    auto vattr_id = constant_predicates_[i].get_vertex_attribute_id();
    auto vattr_value = constant_predicates_[i].get_constant_value();
    const auto& attr_bucket_by_vlabel =
        index_collection_.GetAttributeBucketByVertexLabel(vlabel);
    std::unordered_set<VertexID> valid_vertices =
        attr_bucket_by_vlabel.at(vattr_id).at(vattr_value);
    std::unordered_set<VertexID> diff;
    SetIntersection(&valid_centers, &valid_vertices, &diff);
  }
  return valid_centers;
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

void StarRule::Backup(const MiniCleanCSRGraph& graph,
                      const VertexAttributeID& vertex_attr_id) {
  std::list<std::vector<std::unordered_set<VertexID>>> bucket_diff;
  bucket_diff.resize(1);
  bucket_diff.front().resize(valid_vertex_buckets_.size());
  for (auto& bucket : valid_vertex_buckets_) {
    for (auto it = bucket.begin(); it != bucket.end();) {
      bool match_status = TestStarRule(graph, *it);
      if (match_status) {
        it++;
      } else {
        // Move this vertex to the diff bucket.
        bucket_diff.front()[vertex_attr_id].emplace(*it);
        it = bucket.erase(it);
      }
    }
  }
  valid_vertex_bucket_diffs_.splice(valid_vertex_bucket_diffs_.end(),
                                    bucket_diff);
}

void StarRule::Recover() {
  if (valid_vertex_bucket_diffs_.empty()) {
    LOG_FATAL("No bucket diffs to recover.");
  }
  auto latest_diff = valid_vertex_bucket_diffs_.back();
  for (size_t i = 0; i < latest_diff.size(); i++) {
    for (const auto& vid : latest_diff[i]) {
      valid_vertex_buckets_[i].insert(vid);
    }
  }
  valid_vertex_bucket_diffs_.pop_back();
}

bool StarRule::TestStarRule(const MiniCleanCSRGraph& graph,
                            VertexID center_id) const {
  // Group path rules according to their path pattern ids.
  std::map<PathPatternID, std::vector<PathRule>> path_rule_map;
  for (const auto& path_rule : path_rules_) {
    auto path_pattern_id = path_rule.get_path_pattern_id();
    path_rule_map[path_pattern_id].emplace_back(path_rule);
  }
  // Procss path rules that have the same path pattern id.
  for (const auto& path_rule_pair : path_rule_map) {
    auto path_pattern_id = path_rule_pair.first;
    auto path_rules = path_rule_pair.second;
    const auto& path_instances =
        index_collection_.GetPathInstanceBucket(center_id, path_pattern_id);

    // Check whether the path instances are enough.
    if (path_instances.size() < path_rules.size()) return false;

    std::vector<size_t> match_stack;
    match_stack.reserve(path_rules.size());

    size_t start_pos =
        path_rules[0].GetPathRuleIndex(graph, path_instances, match_stack, 0);
    if (start_pos == path_instances.size()) return false;
    match_stack.push_back(start_pos);

    while (match_stack.size() > 0) {
      // Check whether the match has finished.
      if (match_stack.size() == path_rules.size()) break;
      // Check the next path rule.
      size_t next_matched_position =
          path_rules[match_stack.size()].GetPathRuleIndex(graph, path_instances,
                                                          match_stack, 0);
      while (match_stack.size() > 0 &&
             next_matched_position == path_instances.size()) {
        next_matched_position =
            path_rules[match_stack.size() - 1].GetPathRuleIndex(
                graph, path_instances, match_stack, match_stack.back() + 1);
        match_stack.pop_back();
      }
      if (next_matched_position == path_instances.size()) return false;
      match_stack.push_back(next_matched_position);
    }
  }
  return true;
}
}  // namespace sics::graph::miniclean::data_structures::gcr