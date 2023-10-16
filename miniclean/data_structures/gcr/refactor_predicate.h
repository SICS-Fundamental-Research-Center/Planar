#ifndef MINICLEAN_DATA_STRUCTURES_GCR_R_PREDICATE_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_R_PREDICATE_H_

#include <yaml-cpp/yaml.h>

#include "core/util/logging.h"
#include "miniclean/common/types.h"

namespace xyz::graph::miniclean::data_structures::gcr::refactor {

typedef enum {
  kEq = 0,
  kGt,
} OperatorType;

// x.A [op] c
class ConstantPredicate {
 private:
  using VertexAttributeID = xyz::graph::miniclean::common::VertexAttributeID;
  using VertexLabel = xyz::graph::miniclean::common::VertexLabel;

 public:
  ConstantPredicate() = default;
  ConstantPredicate(VertexLabel vertex_label,
                    VertexAttributeID vertex_attribute_id,
                    uint8_t operator_type, size_t constant_value)
      : vertex_label_(vertex_label),
        vertex_attribute_id_(vertex_attribute_id),
        operator_type_(static_cast<OperatorType>(operator_type)),
        constant_value_(constant_value) {}

  VertexLabel get_vertex_label() const { return vertex_label_; }
  VertexAttributeID get_vertex_attribute_id() const {
    return vertex_attribute_id_;
  }
  OperatorType get_operator_type() const { return operator_type_; }
  size_t get_constant_value() const { return constant_value_; }

  void set_vertex_label(VertexLabel vertex_label) {
    vertex_label_ = vertex_label;
  }
  void set_vertex_attribute_id(VertexAttributeID vertex_attribute_id) {
    vertex_attribute_id_ = vertex_attribute_id;
  }
  void set_operator_type(OperatorType operator_type) {
    operator_type_ = operator_type;
  }
  void set_constant_value(size_t constant_value) {
    constant_value_ = constant_value;
  }

 private:
  VertexLabel vertex_label_;
  VertexAttributeID vertex_attribute_id_;
  OperatorType operator_type_;
  size_t constant_value_;
};

class VariablePredicate {
 private:
  using VertexAttributeID = xyz::graph::miniclean::common::VertexAttributeID;
  using VertexLabel = xyz::graph::miniclean::common::VertexLabel;

 public:
  VariablePredicate() = default;
  VariablePredicate(VertexLabel lhs_label, VertexLabel rhs_label,
                    VertexAttributeID lhs_attribute_id,
                    VertexAttributeID rhs_attribute_id, uint8_t operator_type,
                    std::pair<size_t, size_t> left_attr_range,
                    std::pair<size_t, size_t> right_attr_range)
      : lhs_label_(lhs_label),
        rhs_label_(rhs_label),
        lhs_attribute_id_(lhs_attribute_id),
        rhs_attribute_id_(rhs_attribute_id),
        operator_type_(static_cast<OperatorType>(operator_type)),
        left_attr_range_(left_attr_range),
        right_attr_range_(right_attr_range) {}

  VertexLabel get_lhs_label() const { return lhs_label_; }
  VertexLabel get_rhs_label() const { return rhs_label_; }
  VertexAttributeID get_lhs_attribute_id() const { return lhs_attribute_id_; }
  VertexAttributeID get_rhs_attribute_id() const { return rhs_attribute_id_; }
  OperatorType get_operator_type() const { return operator_type_; }
  std::pair<size_t, size_t> get_left_attr_range() const {
    return left_attr_range_;
  }
  std::pair<size_t, size_t> get_right_attr_range() const {
    return right_attr_range_;
  }

  void set_lhs_label(VertexLabel lhs_label) { lhs_label_ = lhs_label; }
  void set_rhs_label(VertexLabel rhs_label) { rhs_label_ = rhs_label; }
  void set_lhs_attribute_id(VertexAttributeID lhs_attribute_id) {
    lhs_attribute_id_ = lhs_attribute_id;
  }
  void set_rhs_attribute_id(VertexAttributeID rhs_attribute_id) {
    rhs_attribute_id_ = rhs_attribute_id;
  }
  void set_operator_type(OperatorType operator_type) {
    operator_type_ = operator_type;
  }
  void set_left_attr_range(const std::pair<size_t, size_t>& left_attr_range) {
    left_attr_range_ = left_attr_range;
  }
  void set_right_attr_range(const std::pair<size_t, size_t>& right_attr_range) {
    right_attr_range_ = right_attr_range;
  }

 private:
  VertexLabel lhs_label_;
  VertexLabel rhs_label_;
  VertexAttributeID lhs_attribute_id_;
  VertexAttributeID rhs_attribute_id_;
  OperatorType operator_type_;
  std::pair<size_t, size_t> left_attr_range_;
  std::pair<size_t, size_t> right_attr_range_;
};

class ConcreteVariablePredicate {
 private:
  using VertexAttributeID = xyz::graph::miniclean::common::VertexAttributeID;
  using VertexLabel = xyz::graph::miniclean::common::VertexLabel;

 public:
  ConcreteVariablePredicate() = default;
  ConcreteVariablePredicate(VariablePredicate variable_predicate,
                            uint8_t left_path_index, uint8_t left_vertex_index,
                            uint8_t right_path_index,
                            uint8_t right_vertex_index)
      : variable_predicate_(variable_predicate),
        left_path_index_(left_path_index),
        left_vertex_index_(left_vertex_index),
        right_path_index_(right_path_index),
        right_vertex_index_(right_vertex_index) {}

  VariablePredicate get_variable_predicate() const {
    return variable_predicate_;
  }
  uint8_t get_left_path_index() const { return left_path_index_; }
  uint8_t get_left_vertex_index() const { return left_vertex_index_; }
  uint8_t get_right_path_index() const { return right_path_index_; }
  uint8_t get_right_vertex_index() const { return right_vertex_index_; }

  VertexAttributeID get_left_attribute_id() const {
    return variable_predicate_.get_lhs_attribute_id();
  }
  VertexAttributeID get_right_attribute_id() const {
    return variable_predicate_.get_rhs_attribute_id();
  }

  OperatorType get_operator_type() const {
    return variable_predicate_.get_operator_type();
  }

 private:
  VariablePredicate variable_predicate_;
  uint8_t left_path_index_;
  uint8_t left_vertex_index_;
  uint8_t right_path_index_;
  uint8_t right_vertex_index_;
};
}  // namespace xyz::graph::miniclean::data_structures::gcr::refactor

namespace YAML {
template <>
struct convert<
    xyz::graph::miniclean::data_structures::gcr::refactor::ConstantPredicate> {
  static Node encode(const xyz::graph::miniclean::data_structures::gcr::
                         refactor::ConstantPredicate& constant_predicate) {
    Node node;
    node["vertex_label"] = constant_predicate.get_vertex_label();
    node["vertex_attribute_id"] = constant_predicate.get_vertex_attribute_id();
    node["operator_type"] =
        static_cast<uint8_t>(constant_predicate.get_operator_type());
    node["constant_value"] = constant_predicate.get_constant_value();
    return node;
  }

  static bool decode(
      const Node& node,
      xyz::graph::miniclean::data_structures::gcr::refactor::ConstantPredicate&
          constant_predicate) {
    if (node.size() != 4) {
      return false;
    }

    constant_predicate = xyz::graph::miniclean::data_structures::gcr::
        refactor::ConstantPredicate(
            node["vertex_label"]
                .as<xyz::graph::miniclean::common::VertexLabel>(),
            node["vertex_attribute_id"]
                .as<xyz::graph::miniclean::common::VertexAttributeID>(),
            node["operator_type"].as<uint8_t>(),
            node["constant_value"].as<size_t>());
    return true;
  }
};

template <>
struct convert<
    xyz::graph::miniclean::data_structures::gcr::refactor::VariablePredicate> {
  struct Node encode(const xyz::graph::miniclean::data_structures::gcr::
                         refactor::VariablePredicate& variable_predicate) {
    Node node;
    node["lhs_label"] = variable_predicate.get_lhs_label();
    node["rhs_label"] = variable_predicate.get_rhs_label();
    node["lhs_attribute_id"] = variable_predicate.get_lhs_attribute_id();
    node["rhs_attribute_id"] = variable_predicate.get_rhs_attribute_id();
    node["operator_type"] =
        static_cast<uint8_t>(variable_predicate.get_operator_type());
    node["left_attr_range"] = variable_predicate.get_left_attr_range();
    node["right_attr_range"] = variable_predicate.get_right_attr_range();
    return node;
  }

  static bool decode(
      const Node& node,
      xyz::graph::miniclean::data_structures::gcr::refactor::VariablePredicate&
          variable_predicate) {
    if (node.size() != 7) {
      return false;
    }

    variable_predicate = xyz::graph::miniclean::data_structures::gcr::
        refactor::VariablePredicate(
            node["lhs_label"].as<xyz::graph::miniclean::common::VertexLabel>(),
            node["rhs_label"].as<xyz::graph::miniclean::common::VertexLabel>(),
            node["lhs_attribute_id"]
                .as<xyz::graph::miniclean::common::VertexAttributeID>(),
            node["rhs_attribute_id"]
                .as<xyz::graph::miniclean::common::VertexAttributeID>(),
            node["operator_type"].as<uint8_t>(),
            node["left_attr_range"].as<std::pair<size_t, size_t>>(),
            node["right_attr_range"].as<std::pair<size_t, size_t>>());
    return true;
  }
};
}  // namespace YAML

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_R_PREDICATE_H_