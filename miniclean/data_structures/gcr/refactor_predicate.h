#ifndef MINICLEAN_DATA_STRUCTURES_GCR_R_PREDICATE_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_R_PREDICATE_H_

#include <yaml-cpp/yaml.h>

#include "core/util/logging.h"
#include "miniclean/common/types.h"

namespace sics::graph::miniclean::data_structures::gcr::refactor {

typedef enum : uint8_t {
  kEq = 0,
  kGt,
} OperatorType;

// x.A [op] c
class ConstantPredicate {
 private:
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;

 public:
  ConstantPredicate() = default;
  ConstantPredicate(VertexLabel vertex_label,
                    VertexAttributeID vertex_attribute_id,
                    OperatorType operator_type)
      : vertex_label_(vertex_label),
        vertex_attribute_id_(vertex_attribute_id),
        operator_type_(operator_type) {}

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
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;

 public:
  VariablePredicate() = default;
  VariablePredicate(VertexLabel lhs_label, VertexLabel rhs_label,
                    VertexAttributeID lhs_attribute_id,
                    VertexAttributeID rhs_attribute_id,
                    OperatorType operator_type)
      : lhs_label_(lhs_label),
        rhs_label_(rhs_label),
        lhs_attribute_id_(lhs_attribute_id),
        rhs_attribute_id_(rhs_attribute_id),
        operator_type_(operator_type) {}

  VertexLabel get_lhs_label() const { return lhs_label_; }
  VertexLabel get_rhs_label() const { return rhs_label_; }
  VertexAttributeID get_lhs_attribute_id() const { return lhs_attribute_id_; }
  VertexAttributeID get_rhs_attribute_id() const { return rhs_attribute_id_; }
  OperatorType get_operator_type() const { return operator_type_; }

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

 private:
  VertexLabel lhs_label_;
  VertexLabel rhs_label_;
  VertexAttributeID lhs_attribute_id_;
  VertexAttributeID rhs_attribute_id_;
  OperatorType operator_type_;
};

class ConcreteVariablePredicate {
 private:
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;

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

  static const bool TestCompatibility(
      const std::vector<ConcreteVariablePredicate>& lhs_predicates,
      const std::vector<ConcreteVariablePredicate>& rhs_predicates);


 private:
  VariablePredicate variable_predicate_;
  uint8_t left_path_index_;
  uint8_t left_vertex_index_;
  uint8_t right_path_index_;
  uint8_t right_vertex_index_;
};
}  // namespace sics::graph::miniclean::data_structures::gcr::refactor

namespace YAML {
template <>
struct convert<
    sics::graph::miniclean::data_structures::gcr::refactor::ConstantPredicate> {
  static Node encode(const sics::graph::miniclean::data_structures::gcr::
                         refactor::ConstantPredicate& constant_predicate) {
    // TODO: Implement it.
    Node node;
    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::miniclean::data_structures::gcr::refactor::ConstantPredicate&
          constant_predicate) {
    uint8_t operator_type_uint8 = node[2].as<uint8_t>();
    sics::graph::miniclean::data_structures::gcr::refactor::OperatorType
        operator_type;
    switch (operator_type_uint8) {
      case 0:
        operator_type = sics::graph::miniclean::data_structures::gcr::refactor::
            OperatorType::kEq;
        break;
      case 1:
        operator_type = sics::graph::miniclean::data_structures::gcr::refactor::
            OperatorType::kGt;
        break;
      default:
        return false;
    }
    constant_predicate = sics::graph::miniclean::data_structures::gcr::
        refactor::ConstantPredicate(
            node[0].as<sics::graph::miniclean::common::VertexLabel>(),
            node[1].as<sics::graph::miniclean::common::VertexAttributeID>(),
            operator_type);
    return true;
  }
};

template <>
struct convert<
    sics::graph::miniclean::data_structures::gcr::refactor::VariablePredicate> {
  struct Node encode(const sics::graph::miniclean::data_structures::gcr::
                         refactor::VariablePredicate& variable_predicate) {
    // TODO: Implement it.
    Node node;
    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::miniclean::data_structures::gcr::refactor::VariablePredicate&
          variable_predicate) {
    int lhs_vattr_id = node[1].as<int>();
    int rhs_vattr_id = node[3].as<int>();
    if (lhs_vattr_id == -1) {
      lhs_vattr_id = MAX_VERTEX_ATTRIBUTE_ID;
    }
    if (rhs_vattr_id == -1) {
      rhs_vattr_id = MAX_VERTEX_ATTRIBUTE_ID;
    }
    uint8_t operator_type_uint8 = node[4].as<uint8_t>();
    sics::graph::miniclean::data_structures::gcr::refactor::OperatorType
        operator_type;
    switch (operator_type_uint8) {
      case 0:
        operator_type = sics::graph::miniclean::data_structures::gcr::refactor::
            OperatorType::kEq;
        break;
      case 1:
        operator_type = sics::graph::miniclean::data_structures::gcr::refactor::
            OperatorType::kGt;
        break;
      default:
        return false;
    }
    variable_predicate = sics::graph::miniclean::data_structures::gcr::
        refactor::VariablePredicate(
            node[0].as<sics::graph::miniclean::common::VertexLabel>(),
            node[2].as<sics::graph::miniclean::common::VertexLabel>(),
            static_cast<sics::graph::miniclean::common::VertexAttributeID>(
                lhs_vattr_id),
            static_cast<sics::graph::miniclean::common::VertexAttributeID>(
                rhs_vattr_id),
            operator_type);
    return true;
  }
};
}  // namespace YAML

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_R_PREDICATE_H_