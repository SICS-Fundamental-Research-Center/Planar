#include "miniclean/data_structures/gcr/gcr.h"

#include <map>

#include "miniclean/common/types.h"
#include "miniclean/data_structures/gcr/predicate.h"

namespace sics::graph::miniclean::data_structures::gcr {

using PathPattern = sics::graph::miniclean::common::PathPattern;
using PathPatternID = sics::graph::miniclean::common::PathPatternID;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using VertexID = sics::graph::miniclean::common::VertexID;

void GCR::Backup(const MiniCleanCSRGraph& graph, bool added_to_left_star) {
  if (added_to_left_star) {
    left_star_.Backup(graph);
  } else {
    right_star_.Backup(graph);
  }
}

void GCR::Recover(bool horizontal_recover) {
  if (horizontal_recover) {
    size_t horizontal_backup_num = horizontal_extension_log_.back();
    horizontal_extension_log_.pop_back();
    for (size_t i = 0; i < horizontal_backup_num; i++) {
      variable_predicates_.pop_back();
    }
  } else {
    bool extend_to_left = vertical_extension_log_.back();
    vertical_extension_log_.pop_back();
    if (extend_to_left) {
      left_star_.RemoveLastPathRule();
      left_star_.Recover();
    } else {
      right_star_.RemoveLastPathRule();
      right_star_.Recover();
    }
  }
  mining_progress_log_.pop_back();
}

void GCR::ExtendVertically(const GCRVerticalExtension& vertical_extension,
                           const MiniCleanCSRGraph& graph,
                           size_t vertical_extension_id,
                           size_t vertical_extension_num) {
  mining_progress_log_.push_back(
      std::make_pair(vertical_extension_id, vertical_extension_num));
  if (vertical_extension.extend_to_left) {
    vertical_extension_log_.push_back(true);
    AddPathRuleToLeftStar(vertical_extension.path_rule);
    // Update vertex buckets.
    Backup(graph, true);
  } else {
    vertical_extension_log_.push_back(false);
    AddPathRuleToRigthStar(vertical_extension.path_rule);
    // Update vertex buckets.
    Backup(graph, false);
  }
}

void GCR::ExtendHorizontally(const GCRHorizontalExtension& horizontal_extension,
                             const MiniCleanCSRGraph& graph,
                             size_t horizontal_extension_id,
                             size_t horizontal_extension_num) {
  set_consequence(horizontal_extension.consequence);
  for (const auto& c_variable_predicate :
       horizontal_extension.variable_predicates) {
    AddVariablePredicateToBack(c_variable_predicate);
  }
  horizontal_extension_log_.push_back(
      horizontal_extension.variable_predicates.size());
  mining_progress_log_.push_back(
      std::make_pair(horizontal_extension_id, horizontal_extension_num));
  const auto& index_collection = left_star_.get_index_collection();
  // Update vertex buckets.
  // Check previous buckets.
  size_t max_bucket_num = 0;
  ConcreteVariablePredicate max_bucket_num_variable_predicate;
  bool should_rebucket = false;
  if (bucket_id_.left_label != MAX_VERTEX_LABEL) {
    max_bucket_num =
        index_collection.GetAttributeBucketByVertexLabel(bucket_id_.left_label)
            .at(bucket_id_.left_attribute_id)
            .size();
  }
  for (const auto& c_variable_predicat :
       horizontal_extension.variable_predicates) {
    auto left_path_index = c_variable_predicat.get_left_path_index();
    auto right_path_index = c_variable_predicat.get_right_path_index();
    auto left_vertex_index = c_variable_predicat.get_left_vertex_index();
    auto right_vertex_index = c_variable_predicat.get_right_vertex_index();
    auto left_label = c_variable_predicat.get_left_label();
    auto right_label = c_variable_predicat.get_right_label();
    auto left_attr_id = c_variable_predicat.get_left_attribute_id();
    auto right_attr_id = c_variable_predicat.get_right_attribute_id();
    if (left_path_index != 0 || left_vertex_index != 0 ||
        right_path_index != 0 || right_vertex_index != 0)
      continue;
    size_t bucket_size =
        index_collection.GetAttributeBucketByVertexLabel(left_label)
            .at(left_attr_id)
            .size();
    if (bucket_size > max_bucket_num) {
      max_bucket_num = bucket_size;
      max_bucket_num_variable_predicate = c_variable_predicat;
      if (bucket_id_.left_label == left_label &&
          bucket_id_.left_attribute_id == left_attr_id &&
          bucket_id_.right_label == right_label &&
          bucket_id_.right_attribute_id == right_attr_id) {
        should_rebucket = false;
      } else {
        should_rebucket = true;
      }
    }
  }
  if (should_rebucket) {
    bucket_id_.left_label = max_bucket_num_variable_predicate.get_left_label();
    bucket_id_.left_attribute_id =
        max_bucket_num_variable_predicate.get_left_attribute_id();
    bucket_id_.right_label =
        max_bucket_num_variable_predicate.get_right_label();
    bucket_id_.right_attribute_id =
        max_bucket_num_variable_predicate.get_right_attribute_id();
    InitializeBuckets(graph, max_bucket_num_variable_predicate);
  }
}

std::pair<size_t, size_t> GCR::ComputeMatchAndSupport(
    const MiniCleanCSRGraph& graph) {
  // Reset support and match.
  support_ = 0;
  match_ = 0;

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
          support_++;
          // Test consequence.
          if (TestVariablePredicate(graph, consequence_, left_vertex,
                                    right_vertex)) {
            match_++;
          }
        }
      }
    }
  }
  return std::make_pair(match_, support_);
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
      if (value == MAX_VERTEX_ATTRIBUTE_VALUE) continue;
      new_left_valid_vertex_bucket[value].emplace(vid);
    }
  }
  for (const auto& right_bucket : right_valid_vertex_bucket) {
    for (const auto& vid : right_bucket) {
      auto value = graph.GetVertexAttributeValuesByLocalID(vid)[right_attr_id];
      if (value == MAX_VERTEX_ATTRIBUTE_VALUE) continue;
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
  const auto& index_collection = left_star_.get_index_collection();

  auto left_path_id = variable_predicate.get_left_path_index();
  auto right_path_id = variable_predicate.get_right_path_index();
  auto left_vertex_id = variable_predicate.get_left_vertex_index();
  auto right_vertex_id = variable_predicate.get_right_vertex_index();
  auto left_label = variable_predicate.get_left_label();
  auto right_label = variable_predicate.get_right_label();
  auto left_attr_id = variable_predicate.get_left_attribute_id();
  auto right_attr_id = variable_predicate.get_right_attribute_id();

  if (left_label == bucket_id_.left_label &&
      left_attr_id == bucket_id_.left_attribute_id &&
      right_label == bucket_id_.right_label &&
      right_attr_id == bucket_id_.right_attribute_id) {
    return true;
  }

  PathPatternID left_pattern_id = 0;
  PathInstanceBucket left_path_instances;
  if (!left_star_.get_path_rules().empty()) {
    left_pattern_id =
        left_star_.get_path_rules()[left_path_id].get_path_pattern_id();
    left_path_instances =
        index_collection.GetPathInstanceBucket(left_vid, left_pattern_id);
  } else {
    left_path_instances = {{left_vid}};
  }
  PathPatternID right_pattern_id = 0;
  PathInstanceBucket right_path_instances;
  if (!right_star_.get_path_rules().empty()) {
    right_pattern_id =
        right_star_.get_path_rules()[right_path_id].get_path_pattern_id();
    right_path_instances =
        index_collection.GetPathInstanceBucket(right_vid, right_pattern_id);
  } else {
    right_path_instances = {{right_vid}};
  }

  for (const auto& left_instance : left_path_instances) {
    for (const auto& right_instance : right_path_instances) {
      VertexID lvid = left_instance[left_vertex_id];
      VertexID rvid = right_instance[right_vertex_id];
      if (left_attr_id == MAX_VERTEX_ATTRIBUTE_ID) {
        if (right_attr_id != MAX_VERTEX_ATTRIBUTE_ID) {
          LOG_FATAL(
              "Left attribute id is MAX_VERTEX_ATTRIBUTE_ID, but right "
              "attribute id is not.");
        }
        return lvid == rvid;
      }
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

std::string GCR::GetInfoString(const std::vector<PathPattern>& path_patterns,
                               size_t match, size_t support,
                               float confidence) const {
  std::stringstream ss;
  if (mining_progress_log_.size() % 2 == 1) LOG_FATAL("Wrong mining progress.");

  ss << "===GCR info===" << std::endl;
  ss << "Thread ID: " << folly::getCurrentThreadID() << std::endl;
  ss << "-------------Progress Info----------------" << std::endl;
  float progress = 0;
  float base = 1;
  for (size_t i = 0; i < mining_progress_log_.size(); i += 2) {
    size_t vertical_extension_id = mining_progress_log_[i].first;
    size_t vertical_extension_num = mining_progress_log_[i].second;
    size_t horizontal_extension_id = mining_progress_log_[i + 1].first;
    size_t horizontal_extension_num = mining_progress_log_[i + 1].second;
    ss << "Level: " << i / 2
       << "; Vertical extension: " << vertical_extension_id << "/"
       << vertical_extension_num
       << "; Horizontal extension: " << horizontal_extension_id << "/"
       << horizontal_extension_num << std::endl;
    progress += base * vertical_extension_id / vertical_extension_num;
    base /= vertical_extension_num;
    progress += base * horizontal_extension_id / horizontal_extension_num;
    base /= horizontal_extension_num;
  }
  progress *= 100;
  ss << "Estimated mining progress: " << progress << "%" << std::endl;
  ss << "-------------------------------------------" << std::endl;
  ss << "Match: " << match << " Support: " << support
     << " Confidence: " << confidence << std::endl;
  ss << "Left star: " << std::endl;
  ss << left_star_.GetInfoString(path_patterns);
  ss << "Right star: " << std::endl;
  ss << right_star_.GetInfoString(path_patterns);
  ss << "Variable predicates: " << std::endl;
  for (const auto& var_pred : variable_predicates_) {
    uint8_t left_path_id = var_pred.get_left_path_index();
    uint8_t left_vertex_id = var_pred.get_left_vertex_index();
    uint8_t right_path_id = var_pred.get_right_path_index();
    uint8_t right_vertex_id = var_pred.get_right_vertex_index();
    VertexAttributeID left_attr_id = var_pred.get_left_attribute_id();
    VertexAttributeID right_attr_id = var_pred.get_right_attribute_id();
    VertexLabel left_label = var_pred.get_left_label();
    VertexLabel right_label = var_pred.get_right_label();
    OperatorType op_type = var_pred.get_operator_type();
    ss << "Left path id: " << static_cast<int>(left_path_id)
       << ", left vertex id: " << static_cast<int>(left_vertex_id)
       << ", right path id: " << static_cast<int>(right_path_id)
       << ", right vertex id: " << static_cast<int>(right_vertex_id)
       << std::endl;
    ss << static_cast<int>(left_label) << "[" << static_cast<int>(left_attr_id)
       << "]";
    if (op_type == OperatorType::kEq) {
      ss << " = ";
    } else if (op_type == OperatorType::kGt) {
      ss << " > ";
    }
    ss << static_cast<int>(right_label) << "["
       << static_cast<int>(right_attr_id) << "]" << std::endl;
    ss << "---------------------" << std::endl;
  }
  ss << "Consequence: " << std::endl;
  uint8_t left_path_id = consequence_.get_left_path_index();
  uint8_t left_vertex_id = consequence_.get_left_vertex_index();
  uint8_t right_path_id = consequence_.get_right_path_index();
  uint8_t right_vertex_id = consequence_.get_right_vertex_index();
  VertexAttributeID left_attr_id = consequence_.get_left_attribute_id();
  VertexAttributeID right_attr_id = consequence_.get_right_attribute_id();
  VertexLabel left_label = consequence_.get_left_label();
  VertexLabel right_label = consequence_.get_right_label();
  OperatorType op_type = consequence_.get_operator_type();
  ss << "Left path id: " << static_cast<int>(left_path_id)
     << ", left vertex id: " << static_cast<int>(left_vertex_id)
     << ", right path id: " << static_cast<int>(right_path_id)
     << ", right vertex id: " << static_cast<int>(right_vertex_id) << std::endl;
  ss << static_cast<int>(left_label) << "[" << static_cast<int>(left_attr_id)
     << "]";
  if (op_type == OperatorType::kEq) {
    ss << " = ";
  } else if (op_type == OperatorType::kGt) {
    ss << " > ";
  }
  ss << static_cast<int>(right_label) << "[" << static_cast<int>(right_attr_id)
     << "]" << std::endl;
  ss << "---------------------" << std::endl;
  ss << "===End of this GCR===" << std::endl;

  return ss.str();
}

}  // namespace sics::graph::miniclean::data_structures::gcr