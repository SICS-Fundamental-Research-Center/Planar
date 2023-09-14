#include "miniclean/data_structures/gcr/path_rule.h"

#include "miniclean/common/types.h"

namespace sics::graph::miniclean::data_structures::gcr {

using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
using VertexAttributeValue =
    sics::graph::miniclean::common::VertexAttributeValue;

void PathRule::InitBitmap(std::vector<std::vector<VertexID>> path_instance,
                          MiniCleanCSRGraph* graph) {
  star_bitmap_.Clear();
  for (auto instance : path_instance) {
    VertexID center_id = instance[0];
    if (star_bitmap_.TestBit(center_id)) {
      continue;
    }
    // Check whether this instance meets the constant predicates.
    bool is_valid = true;
    for (auto constant_predicate : constant_predicates_) {
      VertexID vid = instance[constant_predicate.first];
      ConstantPredicate predicate = constant_predicate.second;
      VertexAttributeID attribute_id = predicate.get_vertex_attribute_id();
      refactor::OperatorType operator_type = predicate.get_operator_type();
      size_t constant_value = predicate.get_constant_value();

      if (operator_type == refactor::OperatorType::kEq) {
        VertexAttributeValue attr_value =
            graph->GetVertexAttributeValuesByLocalID(vid)[attribute_id];
        if (attr_value != constant_value) {
          is_valid = false;
          break;
        }
      } else {
        // TODO (bai-wenchao): implement this.
      }
    }
    if (is_valid) {
      star_bitmap_.SetBit(center_id);
    }
  }
}

bool PathRule::ComposeWith(PathRule& other, size_t max_pred_num) {
  // Check whether path pattern is the same.
  if (path_pattern_ != other.path_pattern_) {
    return false;
  } 
}

}  // namespace sics::graph::miniclean::data_structures::gcr