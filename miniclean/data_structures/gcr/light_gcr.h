#ifndef MINICLEAN_DATA_STRUCTURES_GCR_LIGHT_GCR_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_LIGHT_GCR_H_

#include <yaml-cpp/yaml.h>

#include <string>
#include <vector>

#include "miniclean/common/error_detection_config.h"
#include "miniclean/common/types.h"

namespace sics::graph::miniclean::data_structures::gcr {

typedef enum : uint8_t {
  kEq = 0,
  kGt,
} OpType;

struct AttributedVertex {
  sics::graph::miniclean::common::VertexLabel label;
  std::vector<sics::graph::miniclean::common::VertexAttributeID> attribute_ids;
  std::vector<std::string> attribute_values;
  std::vector<OpType> op_types;
};

struct BinaryPredicate {
  bool lhs_in_left_star;
  bool rhs_in_left_star;
  uint8_t lhs_path_index;
  uint8_t rhs_path_index;
  uint8_t lhs_vertex_index;
  uint8_t rhs_vertex_index;
  sics::graph::miniclean::common::VertexAttributeID lhs_attribute_id;
  sics::graph::miniclean::common::VertexAttributeID rhs_attribute_id;
  OpType op_type;
};

struct Consequence {
  bool is_binary_predicate;
  bool lhs_in_left_star;
  bool rhs_in_left_star;
  uint8_t lhs_path_index;
  uint8_t rhs_path_index;
  uint8_t lhs_vertex_index;
  uint8_t rhs_vertex_index;
  sics::graph::miniclean::common::VertexAttributeID lhs_attribute_id;
  sics::graph::miniclean::common::VertexAttributeID rhs_attribute_id;
  // `lhs_attribute_value` is for const-pred consequence.
  std::string lhs_attribute_value;
  OpType op_type;
};

class LightGCR {
 private:
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;
  using GCRPath = std::vector<AttributedVertex>;

 public:
  const std::vector<GCRPath>& get_left_star_pattern() const {
    return left_star_pattern_;
  }

  const std::vector<GCRPath>& get_right_star_pattern() const {
    return right_star_pattern_;
  }

  const GCRPath& get_left_path_by_index(size_t index) const {
    return left_star_pattern_[index];
  }

  const GCRPath& get_right_path_by_index(size_t index) const {
    return right_star_pattern_[index];
  }

  const std::vector<BinaryPredicate>& get_binary_preconditions() const {
    return binary_preconditions_;
  }

  const BinaryPredicate& get_binary_precondition_by_index(size_t index) const {
    return binary_preconditions_[index];
  }

  const Consequence& get_consequence() const { return consequence_; }

 private:
  std::vector<GCRPath> left_star_pattern_;
  std::vector<GCRPath> right_star_pattern_;
  std::vector<BinaryPredicate> binary_preconditions_;
  Consequence consequence_;
};

}  // namespace sics::graph::miniclean::data_structures::gcr

namespace YAML {

template <>
struct convert<sics::graph::miniclean::data_structures::gcr::LightGCR> {
  static Node encode(
      const sics::graph::miniclean::data_structures::gcr::LightGCR& rhs) {
    Node node;
    node["left_star_pattern"] = rhs.get_left_star_pattern();
    node["right_star_pattern"] = rhs.get_right_star_pattern();
    node["binary_preconditions"] = rhs.get_binary_preconditions();
    node["consequence"] = rhs.get_consequence();
    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::miniclean::data_structures::gcr::LightGCR& rhs) {
    // TODO (bai-wenchao): implement it.
    return true;
  }
};

template <>
struct convert<sics::graph::miniclean::data_structures::gcr::AttributedVertex> {
  static Node encode(
      const sics::graph::miniclean::data_structures::gcr::AttributedVertex&
          rhs) {
    Node node;
    node["label"] = rhs.label;
    if (rhs.attribute_ids.size() > 0) {
      for (size_t i = 0; i < rhs.attribute_ids.size(); ++i) {
        Node const_pred_node;
        std::string attribute_name =
            sics::graph::miniclean::common::ErrorDetectionConfig::Get()
                ->GetAttrNameByID(rhs.attribute_ids[i]);
        std::string operator_type;
        switch (rhs.op_types[i]) {
          case sics::graph::miniclean::data_structures::gcr::OpType::kEq:
            operator_type = "=";
            break;
          case sics::graph::miniclean::data_structures::gcr::OpType::kGt:
            operator_type = ">";
            break;
          default:
            operator_type = "unknown";
            break;
        }
        const_pred_node["attribute_name"].push_back(attribute_name);
        const_pred_node["operator_type"].push_back(operator_type);
        const_pred_node["attribute_value"].push_back(rhs.attribute_values[i]);
        node["constant_predicates"].push_back(const_pred_node);
      }
    }
    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::miniclean::data_structures::gcr::AttributedVertex& rhs) {
    // TODO (bai-wenchao): implement it.
    return true;
  }
};

template <>
struct convert<sics::graph::miniclean::data_structures::gcr::BinaryPredicate> {
  static Node encode(
      const sics::graph::miniclean::data_structures::gcr::BinaryPredicate&
          rhs) {
    Node node;
    Node left_side;
    Node right_side;
    std::string lhs_attribute_name =
        sics::graph::miniclean::common::ErrorDetectionConfig::Get()
            ->GetAttrNameByID(rhs.lhs_attribute_id);
    std::string rhs_attribute_name =
        sics::graph::miniclean::common::ErrorDetectionConfig::Get()
            ->GetAttrNameByID(rhs.rhs_attribute_id);

    left_side['star_position'] = rhs.lhs_in_left_star ? "left" : "right";
    left_side['path_index'] = rhs.lhs_path_index;
    left_side['vertex_index'] = rhs.lhs_vertex_index;
    left_side['attribute_name'] = lhs_attribute_name;
    node['lhs'] = left_side;

    std::string operator_type;
    switch (rhs.op_type) {
      case sics::graph::miniclean::data_structures::gcr::OpType::kEq:
        operator_type = "=";
        break;
      case sics::graph::miniclean::data_structures::gcr::OpType::kGt:
        operator_type = ">";
        break;
      default:
        operator_type = "unknown";
        break;
    }
    node["operator_type"] = operator_type;

    right_side['star_position'] = rhs.rhs_in_left_star ? "left" : "right";
    right_side['path_index'] = rhs.rhs_path_index;
    right_side['vertex_index'] = rhs.rhs_vertex_index;
    right_side['attribute_name'] = rhs_attribute_name;
    node['rhs'] = right_side;

    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::miniclean::data_structures::gcr::BinaryPredicate& rhs) {
    // TODO (bai-wenchao): implement it.
    return true;
  }
};

template <>
struct convert<sics::graph::miniclean::data_structures::gcr::Consequence> {
  static Node encode(
      const sics::graph::miniclean::data_structures::gcr::Consequence& rhs) {
    Node node;
    Node left_side;
    std::string lhs_attribute_name =
        sics::graph::miniclean::common::ErrorDetectionConfig::Get()
            ->GetAttrNameByID(rhs.lhs_attribute_id);
    left_side['star_position'] = rhs.lhs_in_left_star ? "left" : "right";
    left_side['path_index'] = rhs.lhs_path_index;
    left_side['vertex_index'] = rhs.lhs_vertex_index;
    left_side['attribute_name'] = lhs_attribute_name;
    node['lhs'] = left_side;

    std::string operator_type;
    switch (rhs.op_type) {
      case sics::graph::miniclean::data_structures::gcr::OpType::kEq:
        operator_type = "=";
        break;
      case sics::graph::miniclean::data_structures::gcr::OpType::kGt:
        operator_type = ">";
        break;
      default:
        operator_type = "unknown";
        break;
    }
    node["operator_type"] = operator_type;

    if (rhs.is_binary_predicate) {
      Node right_side;
      std::string rhs_attribute_name =
          sics::graph::miniclean::common::ErrorDetectionConfig::Get()
              ->GetAttrNameByID(rhs.rhs_attribute_id);
      right_side['star_position'] = rhs.rhs_in_left_star ? "left" : "right";
      right_side['path_index'] = rhs.rhs_path_index;
      right_side['vertex_index'] = rhs.rhs_vertex_index;
      right_side['attribute_name'] = rhs_attribute_name;
      node['rhs'] = right_side;
    } else {
      node['rhs'] = rhs.lhs_attribute_value;
    }

    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::miniclean::data_structures::gcr::Consequence& rhs) {
    // TODO (bai-wenchao): implement it.
    return true;
  }
};

}  // namespace YAML

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_LIGHT_GCR_H_