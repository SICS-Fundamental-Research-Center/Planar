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

void GCR::Backup(const MiniCleanCSRGraph& graph,
                 const VertexAttributeID& left_vertex_attr_id,
                 const VertexAttributeID& right_vertex_attr_id) {
  left_star_.Backup(graph, left_vertex_attr_id);
  right_star_.Backup(graph, right_vertex_attr_id);
}

void GCR::Recover() {
  left_star_.Recover();
  right_star_.Recover();
}

void GCR::ExtendVertically(const GCRVerticalExtension& vertical_extension,
                           const MiniCleanCSRGraph& graph) {
  if (vertical_extension.extend_to_left) {
    AddPathRuleToLeftStar(vertical_extension.path_rule);
  } else {
    AddPathRuleToRigthStar(vertical_extension.path_rule);
  }
  // Update vertex buckets.
  Backup(graph, bucket_id_.left_attribute_id, bucket_id_.right_attribute_id);
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

const std::pair<size_t, size_t>& GCR::ComputeMatchAndSupport(
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
  auto left_label = c_variable_predicate.get_left_label();
  auto left_attr_id = c_variable_predicate.get_left_attribute_id();
  const auto& left_label_buckets =
      index_collection.GetAttributeBucketByVertexLabel(left_label);
  auto right_label = c_variable_predicate.get_right_label();
  auto right_attr_id = c_variable_predicate.get_right_attribute_id();
  const auto& right_label_buckets =
      index_collection.GetAttributeBucketByVertexLabel(right_label);
  auto left_value_bucket_size = left_label_buckets.at(left_attr_id).size();
  auto right_value_bucket_size = right_label_buckets.at(right_attr_id).size();

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
      auto value = graph.GetVertexAttributeValuesByLocalID(vid)[left_attr_id];
      new_left_valid_vertex_bucket[value].emplace(vid);
    }
  }
  for (const auto& right_bucket : right_valid_vertex_bucket) {
    for (const auto& vid : right_bucket) {
      auto value = graph.GetVertexAttributeValuesByLocalID(vid)[right_attr_id];
      new_right_valid_vertex_bucket[value].emplace(vid);
    }
  }

  left_star_.UpdateValidVertexBucket(std::move(new_left_valid_vertex_bucket));
  right_star_.UpdateValidVertexBucket(std::move(new_right_valid_vertex_bucket));
}

bool GCR::TestVariablePredicate(
    const MiniCleanCSRGraph& graph,
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

  const auto& index_collection = left_star_.get_index_collection();
  const auto& left_path_instances =
      index_collection.GetPathInstanceBucket(left_vid, left_pattern_id);
  const auto& right_path_instances =
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