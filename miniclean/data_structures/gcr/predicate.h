#ifndef MINICLEAN_DATA_STRUCTURES_GCR_PREDICATE_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_PREDICATE_H_

#include <yaml-cpp/yaml.h>

#include <memory>

#include "core/util/logging.h"
#include "miniclean/common/types.h"

namespace sics::graph::miniclean::data_structures::gcr {

typedef enum {
  kVariablePredicate = 1,
  kConstantPredicate,
  kEdgePredicate,
  kTemporalPredicate,
  kMLPredicate,
} PredicateType;

typedef enum {
  kEqual = 1,
  kNotEqual,
  kLessThan,
  kLessThanOrEqual,
  kGreaterThan,
  kGreaterThanOrEqual,
} OperatorType;

class GCRPredicate {
 protected:
  using PatternVertexID = sics::graph::miniclean::common::PatternVertexID;
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
  using VertexAttributeValue =
      sics::graph::miniclean::common::VertexAttributeValue;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;

 protected:
  GCRPredicate(){};
  GCRPredicate(uint8_t predicate_type, uint8_t operator_type)
      : predicate_type_(static_cast<PredicateType>(predicate_type)),
        operator_type_(static_cast<OperatorType>(operator_type)) {}

  PredicateType predicate_type_;
  OperatorType operator_type_;

 public:
  // Compare attributes.
  //   We assume that the attributes are represented by their ids.
  bool Compare(const VertexAttributeValue& lhs,
               const VertexAttributeValue& rhs) {
    switch (operator_type_) {
      case kEqual:
        return lhs == rhs;
      case kNotEqual:
        return lhs != rhs;
      case kLessThan:
        return lhs < rhs;
      case kLessThanOrEqual:
        return lhs <= rhs;
      case kGreaterThan:
        return lhs > rhs;
      case kGreaterThanOrEqual:
        return lhs >= rhs;
      default:
        return false;
    }
  }

  PredicateType get_predicate_type() const { return predicate_type_; }
  OperatorType get_operator_type() const { return operator_type_; }
};

/* Variable predicate: x.A [op] y.B */
class VariablePredicate : public GCRPredicate {
 public:
  VariablePredicate() {}
  VariablePredicate(uint8_t operator_type, VertexLabel lhs_vlabel,
                    VertexAttributeID lhs_aid, VertexLabel rhs_vlabel,
                    VertexAttributeID rhs_aid)
      : GCRPredicate(1, operator_type),
        lhs_aid_(lhs_aid),
        rhs_aid_(rhs_aid),
        lhs_vlabel_(lhs_vlabel),
        rhs_vlabel_(rhs_vlabel) {}

  VertexLabel get_lhs_vlabel() const { return lhs_vlabel_; }
  VertexLabel get_rhs_vlabel() const { return rhs_vlabel_; }

  VertexAttributeID get_lhs_aid() const { return lhs_aid_; }
  VertexAttributeID get_rhs_aid() const { return rhs_aid_; }

 private:
  VertexAttributeID lhs_aid_, rhs_aid_;
  VertexLabel lhs_vlabel_, rhs_vlabel_;
};

/* Constant predicate: x.A [op] c */
class ConstantPredicate : public GCRPredicate {
 public:
  ConstantPredicate() {}
  ConstantPredicate(uint8_t operator_type, VertexLabel lhs_vlabel,
                    VertexAttributeID lhs_aid, VertexAttributeValue c)
      : GCRPredicate(2, operator_type),
        lhs_vlabel_(lhs_vlabel),
        lhs_aid_(lhs_aid),
        c_(c) {}

  VertexAttributeID get_lhs_vlabel() const { return lhs_vlabel_; }
  VertexAttributeID get_lhs_aid() const { return lhs_aid_; }

  VertexAttributeID get_rhs_value() const { return c_; }

 private:
  VertexLabel lhs_vlabel_;
  VertexAttributeID lhs_aid_;
  VertexAttributeValue c_;
};

}  // namespace sics::graph::miniclean::data_structures::gcr

namespace YAML {
struct convert<sics::graph::miniclean::data_structures::gcr::ConstantPredicate>;
struct convert<sics::graph::miniclean::data_structures::gcr::VariablePredicate>;

template <>
struct convert<
    sics::graph::miniclean::data_structures::gcr::VariablePredicate> {
  static Node encode(
      const sics::graph::miniclean::data_structures::gcr::VariablePredicate&
          variable_predicate) {
    Node node;
    node["operator_type"] =
        static_cast<uint8_t>(variable_predicate.get_operator_type());
    node["lhs_vlabel"] = variable_predicate.get_lhs_vlabel();
    node["rhs_vlabel"] = variable_predicate.get_rhs_vlabel();
    node["lhs_aid"] = variable_predicate.get_lhs_aid();
    node["rhs_aid"] = variable_predicate.get_rhs_aid();
    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::miniclean::data_structures::gcr::VariablePredicate&
          variable_predicate) {
    if (node.size() != 5) {
      return false;
    }

    variable_predicate =
        sics::graph::miniclean::data_structures::gcr::VariablePredicate(
            node["operator_type"].as<uint8_t>(),
            node["lhs_vlabel"]
                .as<sics::graph::miniclean::common::VertexLabel>(),
            node["lhs_aid"]
                .as<sics::graph::miniclean::common::VertexAttributeID>(),
            node["rhs_vlabel"]
                .as<sics::graph::miniclean::common::VertexLabel>(),
            node["rhs_aid"]
                .as<sics::graph::miniclean::common::VertexAttributeID>());
    return true;
  }
};

template <>
struct convert<
    sics::graph::miniclean::data_structures::gcr::ConstantPredicate> {
  static Node encode(
      const sics::graph::miniclean::data_structures::gcr::ConstantPredicate&
          constant_predicate) {
    Node node;
    node["operator_type"] =
        static_cast<uint8_t>(constant_predicate.get_operator_type());
    node["lhs_vlabel"] = constant_predicate.get_lhs_vlabel();
    node["lhs_aid"] = constant_predicate.get_lhs_aid();
    node["rhs"] = constant_predicate.get_rhs_value();
    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::miniclean::data_structures::gcr::ConstantPredicate&
          constant_predicate) {
    if (node.size() != 4) {
      return false;
    }
    constant_predicate =
        sics::graph::miniclean::data_structures::gcr::ConstantPredicate(
            node["operator_type"].as<uint8_t>(),
            node["lhs_vlabel"]
                .as<sics::graph::miniclean::common::VertexLabel>(),
            node["lhs_aid"]
                .as<sics::graph::miniclean::common::VertexAttributeID>(),
            node["rhs_value"]
                .as<sics::graph::miniclean::common::VertexAttributeValue>());
    return true;
  }
};

}  // namespace YAML

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_PREDICATE_H_