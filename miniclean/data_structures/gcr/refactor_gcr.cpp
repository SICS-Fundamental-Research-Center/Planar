#include "miniclean/data_structures/gcr/refactor_gcr.h"

#include <map>

#include "miniclean/common/types.h"
#include "miniclean/components/preprocessor/star_bitmap.h"
#include "miniclean/data_structures/gcr/refactor_predicate.h"

namespace sics::graph::miniclean::data_structures::gcr::refactor {

using StarBitmap = sics::graph::miniclean::components::preprocessor::StarBitmap;
using PathPattern = sics::graph::miniclean::common::PathPattern;
using PathPatternID = sics::graph::miniclean::common::PathPatternID;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using VertexID = sics::graph::miniclean::common::VertexID;

void GCR::Backup() {
  // TODO: Implement it.
}

void GCR::Recover() {
  // TODO: Implement it.
}

void GCR::ExtendVertically(const GCRVerticalExtension& vertical_extension,
                         const MiniCleanCSRGraph& graph) {
  if (vertical_extension.extend_to_left) {
    AddPathRuleToLeftStar(vertical_extension.path_rule);
  } else {
    AddPathRuleToRigthStar(vertical_extension.path_rule);
  }

  auto index_collection = left_star_.get_index_collection();
  // Update left valid vertices.
  for (const auto& bucket : left_star_.get_valid_vertex_bucket()) {
    for (const auto& vid : bucket) {
      bool match_status = TestStarRule(graph, left_star_, vid);
      if (!match_status) {
        // Move this vertex to the diff bucket.
      }
    }
  }
  // Update right valid vertices.
  for (const auto& bucket : right_star_.get_valid_vertex_bucket()) {
    for (const auto& vid : bucket) {
      bool match_status = TestStarRule(graph, right_star_, vid);
      if (!match_status) {
        // Move this vertex to the diff bucket.
      }
    }
  }
}

void GCR::ExtendHorizontally(const GCRHorizontalExtension& horizontal_extension,
                             const MiniCleanCSRGraph& graph) {
  set_consequence(horizontal_extension.consequence);
  for (const auto& c_variable_predicate :
       horizontal_extension.variable_predicates) {
    AddVariablePredicateToBack(c_variable_predicate);
  }
  // Update vertex buckets.
  // 1. Check whether the previous buckets can be reused.
  if (bucket_id_.left_label != MAX_VERTEX_ID) {
    // Have initialized buckets.
    for (const auto& c_variable_predicate :
         horizontal_extension.variable_predicates) {
      if (c_variable_predicate.get_left_label() == bucket_id_.left_label &&
          c_variable_predicate.get_left_attribute_id() ==
              bucket_id_.left_attribute_id &&
          c_variable_predicate.get_right_label() == bucket_id_.right_label &&
          c_variable_predicate.get_right_attribute_id() ==
              bucket_id_.right_attribute_id) {
        // Can reuse the buckets, return directly.
        return;
      }
    }
  }
  // 2. Re-bucketing.
  // TODO: Determine which variable predicate is the best choice to
  // re-bucketing.
  for (const auto& c_variable_predicate :
       horizontal_extension.variable_predicates) {
    // Check whether the predicate located at the centers.
    if (c_variable_predicate.get_left_path_index() != 0 ||
        c_variable_predicate.get_left_vertex_index() != 0 ||
        c_variable_predicate.get_right_path_index() != 0 ||
        c_variable_predicate.get_right_vertex_index() != 0)
      continue;
    // Set the bucket id.
    bucket_id_.left_label = c_variable_predicate.get_left_label();
    bucket_id_.left_attribute_id = c_variable_predicate.get_left_attribute_id();
    bucket_id_.right_label = c_variable_predicate.get_right_label();
    bucket_id_.right_attribute_id =
        c_variable_predicate.get_right_attribute_id();
    // Initialize buckets.
    InitializeBuckets(graph, c_variable_predicate);
    return;
  }
}

std::pair<size_t, size_t> GCR::ComputeMatchAndSupport(
    const MiniCleanCSRGraph& graph) {
  size_t support = 0;
  size_t match = 0;

  bool preconditions_match = true;
  const auto& left_bucket = left_star_.get_valid_vertex_bucket();
  const auto& right_bucket = right_star_.get_valid_vertex_bucket();
  for (size_t i = 0; i < left_bucket.size(); i++) {
    for (const auto& left_vertex : left_bucket[i]) {
      for (const auto& right_vertex : right_bucket[i]) {
        // Test preconditions.
        preconditions_match = true;
        for (const auto& variable_predicate : variable_predicates_) {
          if (!TestVariablePredicate(graph, variable_predicate, left_vertex,
                                     right_vertex)) {
            preconditions_match = false;
            break;
          }
        }
        if (preconditions_match) {
          support++;
          // Test consequence.
          if (TestVariablePredicate(graph, consequence_, left_vertex,
                                    right_vertex)) {
            match++;
          }
        }
      }
    }
  }
  return std::make_pair(match, support);
}

void GCR::InitializeBuckets(
    const MiniCleanCSRGraph& graph,
    const ConcreteVariablePredicate& c_variable_predicate) {
  const auto& index_collection = left_star_.get_index_collection();
  const auto& left_label = c_variable_predicate.get_left_label();
  const auto& left_attr_id = c_variable_predicate.get_left_attribute_id();
  const auto& left_label_buckets =
      index_collection.GetAttributeBucketByVertexLabel(left_label);
  const auto& right_label = c_variable_predicate.get_right_label();
  const auto& right_attr_id = c_variable_predicate.get_right_attribute_id();
  const auto& right_label_buckets =
      index_collection.GetAttributeBucketByVertexLabel(right_label);
  const auto& left_value_bucket_size =
      left_label_buckets.at(left_attr_id).size();
  const auto& right_value_bucket_size =
      right_label_buckets.at(right_attr_id).size();

  if (left_value_bucket_size != right_value_bucket_size) {
    LOG_FATAL("The value bucket size of left and right are not equal.");
  }

  std::vector<std::unordered_set<VertexID>> new_left_valid_vertex_bucket;
  std::vector<std::unordered_set<VertexID>> new_right_valid_vertex_bucket;

  new_left_valid_vertex_bucket.resize(left_value_bucket_size);
  new_right_valid_vertex_bucket.resize(right_value_bucket_size);

  const auto& left_valid_vertex_bucket = left_star_.get_valid_vertex_bucket();
  const auto& right_valid_vertex_bucket = right_star_.get_valid_vertex_bucket();
  for (const auto& left_bucket : left_valid_vertex_bucket) {
    for (const auto& vid : left_bucket) {
      const auto& value =
          graph.GetVertexAttributeValuesByLocalID(vid)[left_attr_id];
      new_left_valid_vertex_bucket[value].emplace(vid);
    }
  }
  for (const auto& right_bucket : right_valid_vertex_bucket) {
    for (const auto& vid : right_bucket) {
      const auto& value =
          graph.GetVertexAttributeValuesByLocalID(vid)[right_attr_id];
      new_right_valid_vertex_bucket[value].emplace(vid);
    }
  }

  left_star_.UpdateValidVertexBucket(std::move(new_left_valid_vertex_bucket));
  right_star_.UpdateValidVertexBucket(std::move(new_right_valid_vertex_bucket));
}

bool GCR::TestStarRule(const MiniCleanCSRGraph& graph,
                       const StarRule& star_rule, VertexID center_id) const {
  auto index_collection = star_rule.get_index_collection();
  // Group path rules according to their path pattern ids.
  std::map<PathPatternID, std::vector<PathRule>> path_rule_map;
  for (const auto& path_rule : star_rule.get_path_rules()) {
    auto path_pattern_id = path_rule.get_path_pattern_id();
    path_rule_map[path_pattern_id].emplace_back(path_rule);
  }
  // Procss path rules that have the same path pattern id.
  for (const auto& path_rule_pair : path_rule_map) {
    auto path_pattern_id = path_rule_pair.first;
    auto path_rules = path_rule_pair.second;
    auto path_instances =
        index_collection.GetPathInstanceBucket(center_id, path_pattern_id);

    // Check whether the path instances are enough.
    if (path_instances.size() < path_rules.size()) return false;

    std::vector<size_t> match_stack;
    match_stack.reserve(path_rules.size());

    size_t start_pos =
        TestPathRule(graph, path_rules[0], path_instances, match_stack, 0);
    if (start_pos == path_instances.size()) return false;
    match_stack.push_back(start_pos);

    while (match_stack.size() > 0) {
      // Check whether the match has finished.
      if (match_stack.size() == path_rules.size()) break;
      // Check the next path rule.
      size_t next_matched_position =
          TestPathRule(graph, path_rules[match_stack.size()], path_instances,
                       match_stack, 0);
      while (match_stack.size() > 0 &&
             next_matched_position == path_instances.size()) {
        next_matched_position =
            TestPathRule(graph, path_rules[match_stack.size() - 1],
                         path_instances, match_stack, match_stack.back() + 1);
        match_stack.pop_back();
      }
      if (next_matched_position == path_instances.size()) return false;
      match_stack.push_back(next_matched_position);
    }
  }
  return true;
}

bool GCR::TestVariablePredicate(
    const MiniCleanCSRGraph& graph,
    const ConcreteVariablePredicate& variable_predicate, VertexID left_vid,
    VertexID right_vid) const {
  const auto& left_path_id = variable_predicate.get_left_path_index();
  const auto& right_path_id = variable_predicate.get_right_path_index();
  const auto& left_vertex_id = variable_predicate.get_left_vertex_index();
  const auto& right_vertex_id = variable_predicate.get_right_vertex_index();
  const auto& left_attr_id = variable_predicate.get_left_attribute_id();
  const auto& right_attr_id = variable_predicate.get_right_attribute_id();
  const auto& left_pattern_id =
      left_star_.get_path_rules()[left_path_id].get_path_pattern_id();
  const auto& right_pattern_id =
      left_star_.get_path_rules()[right_path_id].get_path_pattern_id();

  const auto& index_collection = left_star_.get_index_collection();
  const auto& left_path_instances =
      index_collection.GetPathInstanceBucket(left_vid, left_pattern_id);
  const auto& right_path_instances =
      index_collection.GetPathInstanceBucket(right_vid, right_pattern_id);

  for (const auto& left_instance : left_path_instances) {
    for (const auto& right_instance : right_path_instances) {
      VertexID lvid = left_instance[left_vertex_id];
      VertexID rvid = right_instance[right_vertex_id];
      const auto& left_value =
          graph.GetVertexAttributeValuesByLocalID(lvid)[left_attr_id];
      const auto& right_value =
          graph.GetVertexAttributeValuesByLocalID(rvid)[right_attr_id];
      if (variable_predicate.Test(left_value, right_value)) {
        return true;
      }
    }
  }

  return false;
}

size_t GCR::TestPathRule(const MiniCleanCSRGraph& graph,
                         const PathRule& path_rule,
                         const PathInstanceBucket& path_instance_bucket,
                         const std::vector<size_t>& visited,
                         size_t start_pos) const {
  for (size_t i = start_pos; i < path_instance_bucket.size(); i++) {
    // Check whether i has been visited.
    for (const auto& visited_pos : visited) {
      if (visited_pos == i) continue;
    }
    auto path_instance = path_instance_bucket[i];
    auto constant_predicates = path_rule.get_constant_predicates();
    for (const auto& const_pred : constant_predicates) {
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

bool GCR::PathMatching(const PathPattern& path_pattern,
                       const MiniCleanCSRGraph& graph, size_t vertex_id,
                       size_t edge_id) const {
  // Check depth
  if (edge_id == path_pattern.size()) return true;

  VertexID out_degree = graph.GetOutDegreeByLocalID(vertex_id);
  VertexID* out_edges = graph.GetOutgoingEdgesByLocalID(vertex_id);

  for (size_t i = 0; i < out_degree; i++) {
    // Check edge label.
    if (graph.GetVertexLabelByLocalID(out_edges[i]) !=
        std::get<0>(path_pattern[edge_id]))
      continue;
    bool match_status =
        PathMatching(path_pattern, graph, out_edges[i], edge_id + 1);
    if (match_status) return true;
  }

  return false;
}

std::vector<std::vector<std::pair<size_t, size_t>>>
GCR::ComputeVariablePredicateInstances() const {
  std::vector<std::vector<std::pair<size_t, size_t>>> value_pair_vec;
  value_pair_vec.reserve(variable_predicates_.size() + 1);
  size_t var_pred_instances_size = 1;
  for (size_t i = 0; i < variable_predicates_.size(); i++) {
    value_pair_vec.push_back(
        ComputeAttributeValuePair(variable_predicates_[i]));
    var_pred_instances_size *= value_pair_vec[i].size();
  }
  // The last item is the consequence.
  value_pair_vec.push_back(ComputeAttributeValuePair(consequence_));
  var_pred_instances_size *= value_pair_vec[variable_predicates_.size()].size();

  std::vector<std::vector<std::pair<size_t, size_t>>> var_pred_instances;
  var_pred_instances.reserve(var_pred_instances_size);
  std::vector<std::pair<size_t, size_t>> current_value_pair_vec;
  EnumerateVariablePredicateInstances(value_pair_vec, 0, current_value_pair_vec,
                                      &var_pred_instances);
  return var_pred_instances;
}

void GCR::EnumerateVariablePredicateInstances(
    std::vector<std::vector<std::pair<size_t, size_t>>>& value_pair_vec,
    size_t variable_predicate_index,
    std::vector<std::pair<size_t, size_t>>& current_value_pair_vec,
    std::vector<std::vector<std::pair<size_t, size_t>>>* var_pred_instances)
    const {
  if (variable_predicate_index == value_pair_vec.size()) {
    var_pred_instances->push_back(current_value_pair_vec);
    return;
  }
  for (size_t i = 0; i < value_pair_vec[variable_predicate_index].size(); i++) {
    current_value_pair_vec.push_back(
        value_pair_vec[variable_predicate_index][i]);
    EnumerateVariablePredicateInstances(
        value_pair_vec, variable_predicate_index + 1, current_value_pair_vec,
        var_pred_instances);
    current_value_pair_vec.pop_back();
  }
}

std::vector<std::pair<size_t, size_t>> GCR::ComputeAttributeValuePair(
    const ConcreteVariablePredicate& variable_predicate) const {
  // Retrieve the range of the variable value.
  std::pair<size_t, size_t> left_attr_range = std::make_pair(0, 0);
  std::pair<size_t, size_t> right_attr_range = std::make_pair(0, 0);
  std::vector<std::pair<size_t, size_t>> value_pair;
  switch (variable_predicate.get_variable_predicate().get_operator_type()) {
    case OperatorType::kEq: {
      size_t lb = std::max(left_attr_range.first, right_attr_range.first);
      size_t ub = std::min(left_attr_range.second, right_attr_range.second);
      value_pair.reserve(ub - lb + 1);
      for (size_t i = lb; i <= ub; i++) {
        value_pair.emplace_back(i, i);
      }
      break;
    }
    case OperatorType::kGt: {
      if (left_attr_range.second <= right_attr_range.first) break;
      size_t reserve_size =
          (left_attr_range.second - right_attr_range.first + 1) *
          (left_attr_range.second - right_attr_range.first) / 2;
      value_pair.reserve(reserve_size);
      for (size_t i = left_attr_range.first; i <= left_attr_range.second; i++) {
        if (i <= right_attr_range.first) continue;
        for (size_t j = right_attr_range.first; j <= right_attr_range.second;
             j++) {
          if (i > j) {
            value_pair.emplace_back(i, j);
          }
        }
      }
      break;
    }
  }
  return value_pair;
}

bool GCR::IsCompatibleWith(const ConcreteVariablePredicate& variable_predicate,
                           bool consider_consequence) const {
  std::vector<ConcreteVariablePredicate> variable_predicates;
  variable_predicates.reserve(1);
  variable_predicates.emplace_back(variable_predicate);
  bool compatibilty = ConcreteVariablePredicate::TestCompatibility(
      variable_predicates, variable_predicates_);
  if (consider_consequence) {
    std::vector<ConcreteVariablePredicate> consequences;
    consequences.reserve(1);
    consequences.emplace_back(consequence_);
    compatibilty = compatibilty && ConcreteVariablePredicate::TestCompatibility(
                                       variable_predicates, consequences);
  }
  return compatibilty;
}

}  // namespace sics::graph::miniclean::data_structures::gcr::refactor