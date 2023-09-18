#include "miniclean/data_structures/gcr/path_rule.h"

#include "miniclean/common/types.h"

namespace sics::graph::miniclean::data_structures::gcr {

using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
using VertexAttributeValue =
    sics::graph::miniclean::common::VertexAttributeValue;

void PathRule::InitBitmap(std::vector<std::vector<VertexID>> path_instance,
                          MiniCleanCSRGraph* graph) {
  star_bitmap_.Clear();
  for (const auto& instance : path_instance) {
    VertexID center_id = instance[0];
    if (star_bitmap_.TestBit(center_id)) {
      continue;
    }
    // Check whether this instance meets the constant predicates.
    bool is_valid = true;
    for (const auto& constant_predicate : constant_predicates_) {
      VertexID vid = instance[constant_predicate.first];
      ConstantPredicate predicate = constant_predicate.second;
      VertexAttributeID attribute_id = predicate.get_vertex_attribute_id();
      refactor::OperatorType operator_type = predicate.get_operator_type();
      size_t constant_value = predicate.get_constant_value();

      switch (operator_type) {
        case refactor::OperatorType::kEq:
          if (graph->GetVertexAttributeValuesByLocalID(vid)[attribute_id] !=
              constant_value) {
            is_valid = false;
            break;
          }
          break;
        case refactor::OperatorType::kGt:
          LOG_FATAL("Not implemented yet.");
      }
    }
    if (is_valid) {
      star_bitmap_.SetBit(center_id);
    }
  }
}

bool PathRule::ComposeWith(PathRule& other, size_t max_pred_num) {
  // Compose the predicates.
  auto other_constant_predicates = other.get_constant_predicates();
  bool has_composed = false;
  if (other_constant_predicates.size() == 0 ||
      constant_predicates_.size() == 0) {
    return false;
  }

  for (const auto& other_constant_predicate_pair : other_constant_predicates) {
    bool has_same_predicate = false;
    bool is_largest_predicate = true;
    for (const auto& constant_predicate_pair : constant_predicates_) {
      // check whether the current path rule already has this predicate.
      if (constant_predicate_pair.first ==
              other_constant_predicate_pair.first &&
          constant_predicate_pair.second.get_vertex_attribute_id() ==
              other_constant_predicate_pair.second.get_vertex_attribute_id()) {
        has_same_predicate = true;
        break;
      }
      // check whether the order of the predicates.
      size_t this_order =
          constant_predicate_pair.first * 10 +
          constant_predicate_pair.second.get_vertex_attribute_id();
      size_t other_order =
          other_constant_predicate_pair.first * 10 +
          other_constant_predicate_pair.second.get_vertex_attribute_id();
      if (this_order >= other_order) {
        is_largest_predicate = false;
        break;
      }
    }
    if (!has_same_predicate && is_largest_predicate) {
      constant_predicates_.emplace_back(other_constant_predicate_pair);
      has_composed = true;
    }
  }
  if (!has_composed || constant_predicates_.size() > max_pred_num) {
    return false;
  }

  star_bitmap_.MergeWith(other.star_bitmap_);
  return true;
}

}  // namespace sics::graph::miniclean::data_structures::gcr