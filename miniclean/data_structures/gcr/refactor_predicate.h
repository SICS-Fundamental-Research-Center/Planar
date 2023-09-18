#ifndef MINICLEAN_DATA_STRUCTURES_GCR_R_PREDICATE_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_R_PREDICATE_H_

#include <yaml-cpp/yaml.h>

#include "core/util/logging.h"
#include "miniclean/common/types.h"

namespace sics::graph::miniclean::data_structures::gcr::refactor {

typedef enum {
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
}  // namespace sics::graph::miniclean::data_structures::gcr::refactor

namespace YAML {
template <>
struct convert<
    sics::graph::miniclean::data_structures::gcr::refactor::ConstantPredicate> {
  static Node encode(const sics::graph::miniclean::data_structures::gcr::
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
      sics::graph::miniclean::data_structures::gcr::refactor::ConstantPredicate&
          constant_predicate) {
    if (node.size() != 4) {
      return false;
    }

    constant_predicate = sics::graph::miniclean::data_structures::gcr::
        refactor::ConstantPredicate(
            node["vertex_label"]
                .as<sics::graph::miniclean::common::VertexLabel>(),
            node["vertex_attribute_id"]
                .as<sics::graph::miniclean::common::VertexAttributeID>(),
            node["operator_type"].as<uint8_t>(),
            node["constant_value"].as<size_t>());
    return true;
  }
};
}  // namespace YAML

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_R_PREDICATE_H_