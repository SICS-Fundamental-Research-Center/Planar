#include "miniclean/data_structures/gcr/refactor_gcr.h"

#include "miniclean/common/types.h"
#include "miniclean/components/preprocessor/star_bitmap.h"
#include "miniclean/data_structures/gcr/refactor_predicate.h"

namespace sics::graph::miniclean::data_structures::gcr::refactor {

using StarBitmap = sics::graph::miniclean::components::preprocessor::StarBitmap;
using PathPattern = sics::graph::miniclean::common::PathPattern;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using VertexID = sics::graph::miniclean::common::VertexID;

void GCR::Backup() {
  // TODO: Implement it.
}

void GCR::Recover() {
  // TODO: Implement it.
}

void GCR::VerticalExtend(const GCRVerticalExtension& vertical_extension) {
  if (vertical_extension.first) {
    AddPathRuleToLeftStar(vertical_extension.second);
  } else {
    AddPathRuleToRigthStar(vertical_extension.second);
  }
  // Update vertex set.
}

void GCR::HorizontalExtend(const GCRHorizontalExtension& horizontal_extension) {
  set_consequence(horizontal_extension.first);
  for (const auto& variable_predicate : horizontal_extension.second) {
    AddVariablePredicateToBack(variable_predicate);
  }
}

std::pair<size_t, size_t> GCR::ComputeMatchAndSupport(
    MiniCleanCSRGraph& graph) {
  size_t support = 0;
  size_t match = 0;
  InitializeBuckets(graph);
  bool preconditions_match = true;
  auto left_bucket = left_star_.get_bucket();
  auto right_bucket = right_star_.get_bucket();
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

void GCR::InitializeBuckets(MiniCleanCSRGraph& graph) {
  auto index_collection = left_star_.get_index_collection();
  std::unordered_set<VertexID> left_valid_vertices = left_star_.get_valid_vertices();
  std::unordered_set<VertexID> right_valid_vertices = right_star_.get_valid_vertices();
  if (left_star_.get_bucket().size() != right_star_.get_bucket().size()) {
    LOG_FATAL("Number of buckets in left and right star are not the same.");
  }
  if (left_star_.get_bucket().size() == 0) {
    // Initialize buckets.
    for (size_t i = 0; i < variable_predicates_.size(); i++) {
      auto left_path_id = variable_predicates_[i].get_left_path_index();
      auto right_path_id = variable_predicates_[i].get_right_path_index();
      auto left_vertex_id = variable_predicates_[i].get_left_vertex_index();
      auto right_vertex_id = variable_predicates_[i].get_right_vertex_index();
      if (left_path_id != 0 || left_vertex_id != 0 || right_path_id != 0 ||
          right_vertex_id != 0)
        continue;
      bucket_index_ = i;
      auto left_label = variable_predicates_[i].get_left_label();
      auto left_attr_id = variable_predicates_[i].get_left_attribute_id();
      auto left_label_buckts =
          index_collection.GetAttributeBucketByVertexLabel(left_label);
      auto value_bucket_size = left_label_buckts[left_attr_id].size();
      left_star_.ReserveBucket(value_bucket_size);
      right_star_.ReserveBucket(value_bucket_size);
      for (const auto& left_valid_vertex : left_valid_vertices) {
        auto value = graph.GetVertexAttributeValuesByLocalID(
            left_valid_vertex)[left_attr_id];
        left_star_.AddVertexToBucket(value, left_valid_vertex);
      }
      for (const auto& right_valid_vertex : right_valid_vertices) {
        auto value = graph.GetVertexAttributeValuesByLocalID(
            right_valid_vertex)[left_attr_id];
        right_star_.AddVertexToBucket(value, right_valid_vertex);
      }
      break;
    }
  }
  if (left_star_.get_bucket().size() == 0) {
    // All vertices in one bucket.
    left_star_.ReserveBucket(1);
    right_star_.ReserveBucket(1);
    for (const auto& left_valid_vertex : left_valid_vertices) {
      left_star_.AddVertexToBucket(0, left_valid_vertex);
    }
    for (const auto& right_valid_vertex : right_valid_vertices) {
      right_star_.AddVertexToBucket(0, right_valid_vertex);
    }
  }
}

bool GCR::TestStarRule(MiniCleanCSRGraph& graph, const StarRule& star_rule,
                       VertexID center_id) const {
  // TODO: Implement it.
}

bool GCR::TestVariablePredicate(
    MiniCleanCSRGraph& graph,
    const ConcreteVariablePredicate& variable_predicate, VertexID left_vid,
    VertexID right_vid) const {
  auto left_path_id = variable_predicate.get_left_path_index();
  auto right_path_id = variable_predicate.get_right_path_index();
  auto left_vertex_id = variable_predicate.get_left_vertex_index();
  auto right_vertex_id = variable_predicate.get_right_vertex_index();
  auto left_attr_id = variable_predicate.get_left_attribute_id();
  auto right_attr_id = variable_predicate.get_right_attribute_id();
  auto left_pattern_id =
      left_star_.get_path_rules()[left_path_id].get_path_pattern_id();
  auto right_pattern_id =
      left_star_.get_path_rules()[right_path_id].get_path_pattern_id();

  auto left_bucket = left_star_.get_bucket();
  auto right_bucket = right_star_.get_bucket();
  auto index_collection = left_star_.get_index_collection();

  auto left_path_instances =
      index_collection.GetPathInstanceBucket(left_vid, left_pattern_id);
  auto right_path_instances =
      index_collection.GetPathInstanceBucket(right_vid, right_pattern_id);

  for (const auto& left_instance : left_path_instances) {
    for (const auto& right_instance : right_path_instances) {
      VertexID lvid = left_instance[left_vertex_id];
      VertexID rvid = right_instance[right_vertex_id];
      auto left_value =
          graph.GetVertexAttributeValuesByLocalID(lvid)[left_attr_id];
      auto right_value =
          graph.GetVertexAttributeValuesByLocalID(rvid)[right_attr_id];
      if (variable_predicate.Test(left_value, right_value)) {
        return true;
      }
    }
  }

  return false;
}


bool GCR::PathMatching(PathPattern path_pattern, MiniCleanCSRGraph& graph,
                       size_t vertex_id, size_t edge_id) const {
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