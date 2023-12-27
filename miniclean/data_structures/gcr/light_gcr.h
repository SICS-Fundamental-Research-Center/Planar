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
  AttributedVertex() = default;
  AttributedVertex(const sics::graph::miniclean::common::VertexLabel& vlabel_id)
      : label_id(vlabel_id) {}

  sics::graph::miniclean::common::VertexLabel label_id;
  std::vector<sics::graph::miniclean::common::VertexAttributeID> attribute_ids;
  std::vector<std::string> attribute_values;
  std::vector<OpType> op_types;
};

struct BinaryPredicate {
  BinaryPredicate() = default;
  BinaryPredicate(
      bool lhs_in_left_star, bool rhs_in_left_star, uint8_t lhs_path_index,
      uint8_t rhs_path_index, uint8_t lhs_vertex_index,
      uint8_t rhs_vertex_index,
      sics::graph::miniclean::common::VertexAttributeID lhs_attribute_id,
      sics::graph::miniclean::common::VertexAttributeID rhs_attribute_id,
      OpType op_type)
      : lhs_in_left_star(lhs_in_left_star),
        rhs_in_left_star(rhs_in_left_star),
        lhs_path_index(lhs_path_index),
        rhs_path_index(rhs_path_index),
        lhs_vertex_index(lhs_vertex_index),
        rhs_vertex_index(rhs_vertex_index),
        lhs_attribute_id(lhs_attribute_id),
        rhs_attribute_id(rhs_attribute_id),
        op_type(op_type) {}

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
  Consequence() = default;
  // For binary-pred consequence.
  Consequence(
      bool lhs_in_left_star, bool rhs_in_left_star, uint8_t lhs_path_index,
      uint8_t rhs_path_index, uint8_t lhs_vertex_index,
      uint8_t rhs_vertex_index,
      sics::graph::miniclean::common::VertexAttributeID lhs_attribute_id,
      sics::graph::miniclean::common::VertexAttributeID rhs_attribute_id,
      OpType op_type)
      : is_binary_predicate(true),
        lhs_in_left_star(lhs_in_left_star),
        rhs_in_left_star(rhs_in_left_star),
        lhs_path_index(lhs_path_index),
        rhs_path_index(rhs_path_index),
        lhs_vertex_index(lhs_vertex_index),
        rhs_vertex_index(rhs_vertex_index),
        lhs_attribute_id(lhs_attribute_id),
        rhs_attribute_id(rhs_attribute_id),
        rhs_value("\0"),
        op_type(op_type) {}
  // For unary-pred consequence.
  Consequence(
      bool lhs_in_left_star, uint8_t lhs_path_index, uint8_t lhs_vertex_index,
      sics::graph::miniclean::common::VertexAttributeID lhs_attribute_id,
      const std::string& rhs_value, OpType op_type)
      : is_binary_predicate(false),
        lhs_in_left_star(lhs_in_left_star),
        rhs_in_left_star(false),
        lhs_path_index(lhs_path_index),
        rhs_path_index(255),
        lhs_vertex_index(lhs_vertex_index),
        rhs_vertex_index(255),
        lhs_attribute_id(lhs_attribute_id),
        rhs_attribute_id(255),
        rhs_value(rhs_value),
        op_type(op_type) {}

  bool is_binary_predicate;
  bool lhs_in_left_star;
  bool rhs_in_left_star;
  uint8_t lhs_path_index;
  uint8_t rhs_path_index;
  uint8_t lhs_vertex_index;
  uint8_t rhs_vertex_index;
  sics::graph::miniclean::common::VertexAttributeID lhs_attribute_id;
  sics::graph::miniclean::common::VertexAttributeID rhs_attribute_id;
  std::string rhs_value;
  OpType op_type;
};

class LightGCR {
 private:
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;
  using GCRPath = std::vector<AttributedVertex>;

 public:
  LightGCR() = default;
  LightGCR(const std::vector<GCRPath>& left_star_pattern,
           const std::vector<GCRPath>& right_star_pattern,
           const std::vector<BinaryPredicate>& binary_preconditions,
           const Consequence& consequence)
      : left_star_pattern_(left_star_pattern),
        right_star_pattern_(right_star_pattern),
        binary_preconditions_(binary_preconditions),
        consequence_(consequence) {}

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

  void set_left_star_pattern(const std::vector<GCRPath>& left_star_pattern) {
    left_star_pattern_ = left_star_pattern;
  }

  void set_right_star_pattern(const std::vector<GCRPath>& right_star_pattern) {
    right_star_pattern_ = right_star_pattern;
  }

  void set_binary_preconditions(
      const std::vector<BinaryPredicate>& binary_preconditions) {
    binary_preconditions_ = binary_preconditions;
  }

  void set_consequence(const Consequence& consequence) {
    consequence_ = consequence;
  }

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
    if (node.size() != 4) {
      LOGF_FATAL("LightGCR node size {} != 4.", node.size());
    }
    rhs = sics::graph::miniclean::data_structures::gcr::LightGCR(
        node["left_star_pattern"]
            .as<std::vector<
                std::vector<sics::graph::miniclean::data_structures::gcr::
                                AttributedVertex>>>(),
        node["right_star_pattern"]
            .as<std::vector<
                std::vector<sics::graph::miniclean::data_structures::gcr::
                                AttributedVertex>>>(),
        node["binary_preconditions"]
            .as<std::vector<sics::graph::miniclean::data_structures::gcr::
                                BinaryPredicate>>(),
        node["consequence"]
            .as<sics::graph::miniclean::data_structures::gcr::Consequence>());
    return true;
  }
};

template <>
struct convert<sics::graph::miniclean::data_structures::gcr::AttributedVertex> {
  static Node encode(
      const sics::graph::miniclean::data_structures::gcr::AttributedVertex&
          rhs) {
    Node node;
    std::string vlabel =
        sics::graph::miniclean::common::ErrorDetectionConfig::Get()
            ->GetLabelNameByID(rhs.label_id);
    node["label"] = vlabel;
    if (rhs.attribute_ids.size() > 0) {
      for (size_t i = 0; i < rhs.attribute_ids.size(); ++i) {
        Node const_pred_node;
        std::string attribute_name =
            sics::graph::miniclean::common::ErrorDetectionConfig::Get()
                ->GetAttrNameByID(rhs.attribute_ids[i]);
        std::string operator_type;
        switch (rhs.op_types[i]) {
          case sics::graph::miniclean::data_structures::gcr::OpType::kEq:
            operator_type = "Eq";
            break;
          case sics::graph::miniclean::data_structures::gcr::OpType::kGt:
            operator_type = "Gt";
            break;
          default:
            operator_type = "unknown";
            break;
        }
        const_pred_node["attribute_name"] = attribute_name;
        const_pred_node["operator_type"] = operator_type;
        const_pred_node["attribute_value"] = rhs.attribute_values[i];
        node["constant_predicates"].push_back(const_pred_node);
      }
    }
    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::miniclean::data_structures::gcr::AttributedVertex& rhs) {
    rhs = sics::graph::miniclean::data_structures::gcr::AttributedVertex(
        sics::graph::miniclean::common::ErrorDetectionConfig::Get()
            ->GetLabelIDByName(node["label"].as<std::string>()));
    if (node.size() == 1) {
      return true;
    }
    if (node.size() != 2) {
      LOGF_FATAL("AttributedVertex node size {} != 2.", node.size());
    }
    for (size_t i = 0; i < node["constant_predicates"].size(); i++) {
      rhs.attribute_ids.push_back(
          sics::graph::miniclean::common::ErrorDetectionConfig::Get()
              ->GetAttrIDByName(node["constant_predicates"][i]["attribute_name"]
                                    .as<std::string>()));
      rhs.attribute_values.push_back(
          node["constant_predicates"][i]["attribute_value"].as<std::string>());
      rhs.op_types.push_back(
          node["constant_predicates"][i]["operator_type"].as<std::string>() ==
                  "Eq"
              ? sics::graph::miniclean::data_structures::gcr::OpType::kEq
              : sics::graph::miniclean::data_structures::gcr::OpType::kGt);
    }
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

    left_side["star_position"] = rhs.lhs_in_left_star ? "left" : "right";
    left_side["path_index"] = (uint32_t)rhs.lhs_path_index;
    left_side["vertex_index"] = (uint32_t)rhs.lhs_vertex_index;
    left_side["attribute_name"] = lhs_attribute_name;
    node["lhs"] = left_side;

    std::string operator_type;
    switch (rhs.op_type) {
      case sics::graph::miniclean::data_structures::gcr::OpType::kEq:
        operator_type = "Eq";
        break;
      case sics::graph::miniclean::data_structures::gcr::OpType::kGt:
        operator_type = "Gt";
        break;
      default:
        operator_type = "unknown";
        break;
    }
    node["operator_type"] = operator_type;

    right_side["star_position"] = rhs.rhs_in_left_star ? "left" : "right";
    right_side["path_index"] = (uint32_t)rhs.rhs_path_index;
    right_side["vertex_index"] = (uint32_t)rhs.rhs_vertex_index;
    right_side["attribute_name"] = rhs_attribute_name;
    node["rhs"] = right_side;

    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::miniclean::data_structures::gcr::BinaryPredicate& rhs) {
    if (node.size() != 3) {
      LOGF_FATAL("BinaryPredicate node size {} != 3.", node.size());
    }
    rhs = sics::graph::miniclean::data_structures::gcr::BinaryPredicate(
        node["lhs"]["star_position"].as<std::string>() == "left" ? true : false,
        node["rhs"]["star_position"].as<std::string>() == "left" ? true : false,
        node["lhs"]["path_index"].as<uint8_t>(),
        node["rhs"]["path_index"].as<uint8_t>(),
        node["lhs"]["vertex_index"].as<uint8_t>(),
        node["rhs"]["vertex_index"].as<uint8_t>(),
        sics::graph::miniclean::common::ErrorDetectionConfig::Get()
            ->GetAttrIDByName(node["lhs"]["attribute_name"].as<std::string>()),
        sics::graph::miniclean::common::ErrorDetectionConfig::Get()
            ->GetAttrIDByName(node["rhs"]["attribute_name"].as<std::string>()),
        node["operator_type"].as<std::string>() == "Eq"
            ? sics::graph::miniclean::data_structures::gcr::OpType::kEq
            : sics::graph::miniclean::data_structures::gcr::OpType::kGt);
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
    left_side["star_position"] = rhs.lhs_in_left_star ? "left" : "right";
    left_side["path_index"] = (uint32_t)rhs.lhs_path_index;
    left_side["vertex_index"] = (uint32_t)rhs.lhs_vertex_index;
    left_side["attribute_name"] = lhs_attribute_name;
    node["lhs"] = left_side;

    std::string operator_type;
    switch (rhs.op_type) {
      case sics::graph::miniclean::data_structures::gcr::OpType::kEq:
        operator_type = "Eq";
        break;
      case sics::graph::miniclean::data_structures::gcr::OpType::kGt:
        operator_type = "Gt";
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
      right_side["star_position"] = rhs.rhs_in_left_star ? "left" : "right";
      right_side["path_index"] = (uint32_t)rhs.rhs_path_index;
      right_side["vertex_index"] = (uint32_t)rhs.rhs_vertex_index;
      right_side["attribute_name"] = rhs_attribute_name;
      node["rhs"] = right_side;
    } else {
      node["rhs"] = rhs.rhs_value;
    }

    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::miniclean::data_structures::gcr::Consequence& rhs) {
    if (node.size() != 3) {
      LOGF_FATAL("Consequence node size {} != 3.", node.size());
    }
    // Determine whether it is a binary predicate.
    if (node["rhs"].IsScalar()) {
      // Unary predicate.
      rhs = sics::graph::miniclean::data_structures::gcr::Consequence(
          node["lhs"]["star_position"].as<std::string>() == "left" ? true
                                                                   : false,
          node["lhs"]["path_index"].as<uint8_t>(),
          node["lhs"]["vertex_index"].as<uint8_t>(),
          sics::graph::miniclean::common::ErrorDetectionConfig::Get()
              ->GetAttrIDByName(
                  node["lhs"]["attribute_name"].as<std::string>()),
          node["rhs"].as<std::string>(),
          node["operator_type"].as<std::string>() == "Eq"
              ? sics::graph::miniclean::data_structures::gcr::OpType::kEq
              : sics::graph::miniclean::data_structures::gcr::OpType::kGt);
    } else {
      // Binary predicate.
      rhs = sics::graph::miniclean::data_structures::gcr::Consequence(
          node["lhs"]["star_position"].as<std::string>() == "left" ? true
                                                                   : false,
          node["rhs"]["star_position"].as<std::string>() == "left" ? true
                                                                   : false,
          node["lhs"]["path_index"].as<uint8_t>(),
          node["rhs"]["path_index"].as<uint8_t>(),
          node["lhs"]["vertex_index"].as<uint8_t>(),
          node["rhs"]["vertex_index"].as<uint8_t>(),
          sics::graph::miniclean::common::ErrorDetectionConfig::Get()
              ->GetAttrIDByName(
                  node["lhs"]["attribute_name"].as<std::string>()),
          sics::graph::miniclean::common::ErrorDetectionConfig::Get()
              ->GetAttrIDByName(
                  node["rhs"]["attribute_name"].as<std::string>()),
          node["operator_type"].as<std::string>() == "Eq"
              ? sics::graph::miniclean::data_structures::gcr::OpType::kEq
              : sics::graph::miniclean::data_structures::gcr::OpType::kGt);
    }
    return true;
  }
};

}  // namespace YAML

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_LIGHT_GCR_H_