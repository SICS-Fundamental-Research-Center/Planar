#ifndef MINICLEAN_DATA_STRUCTURES_GCR_PREDICATE_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_PREDICATE_H_

#include <yaml-cpp/yaml.h>

#include <memory>

#include "core/util/logging.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

namespace xyz::graph::miniclean::data_structures::gcr {

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
  using PatternVertexID = xyz::graph::miniclean::common::PatternVertexID;
  using MiniCleanCSRGraph =
      xyz::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using VertexAttributeID = xyz::graph::miniclean::common::VertexAttributeID;
  using VertexAttributeValue =
      xyz::graph::miniclean::common::VertexAttributeValue;
  using VertexID = xyz::graph::miniclean::common::VertexID;
  using VertexLabel = xyz::graph::miniclean::common::VertexLabel;
  using PathPatternID = xyz::graph::miniclean::common::PathPatternID;

 public:
  GCRPredicate() = default;
  GCRPredicate(uint8_t predicate_type, uint8_t operator_type)
      : predicate_type_(static_cast<PredicateType>(predicate_type)),
        operator_type_(static_cast<OperatorType>(operator_type)) {}
  GCRPredicate(const GCRPredicate& other_gcr_predicate)
      : predicate_type_(other_gcr_predicate.get_predicate_type()),
        operator_type_(other_gcr_predicate.get_operator_type()) {}

 public:
  // Compare attributes.
  virtual bool Compare(const VertexAttributeValue& lhs,
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
    }
  }

  PredicateType get_predicate_type() const { return predicate_type_; }
  OperatorType get_operator_type() const { return operator_type_; }

 protected:
  PredicateType predicate_type_;
  OperatorType operator_type_;
};

/* Variable predicate: x.A [op] y.B */
class VariablePredicate : public GCRPredicate {
 public:
  VariablePredicate() = default;
  VariablePredicate(uint8_t operator_type, VertexLabel lhs_vlabel,
                    VertexAttributeID lhs_aid, VertexLabel rhs_vlabel,
                    VertexAttributeID rhs_aid)
      : GCRPredicate(1, operator_type),
        lhs_aid_(lhs_aid),
        rhs_aid_(rhs_aid),
        lhs_vlabel_(lhs_vlabel),
        rhs_vlabel_(rhs_vlabel) {}
  VariablePredicate(const VariablePredicate& other_variable_predicate)
      : GCRPredicate(other_variable_predicate.get_predicate_type(),
                     other_variable_predicate.get_operator_type()),
        lhs_aid_(other_variable_predicate.get_lhs_aid()),
        rhs_aid_(other_variable_predicate.get_rhs_aid()),
        lhs_vlabel_(other_variable_predicate.get_lhs_vlabel()),
        rhs_vlabel_(other_variable_predicate.get_rhs_vlabel()) {}

  VertexLabel get_lhs_vlabel() const { return lhs_vlabel_; }
  VertexLabel get_rhs_vlabel() const { return rhs_vlabel_; }

  VertexAttributeID get_lhs_aid() const { return lhs_aid_; }
  VertexAttributeID get_rhs_aid() const { return rhs_aid_; }

  PathPatternID get_lhs_ppid() const { return lhs_ppid_; }
  PathPatternID get_rhs_ppid() const { return rhs_ppid_; }

  size_t get_lhs_vertex_id() const { return lhs_vertex_id_; }
  size_t get_rhs_vertex_id() const { return rhs_vertex_id_; }

  void set_lhs_ppid(PathPatternID lhs_ppid) { lhs_ppid_ = lhs_ppid; }
  void set_rhs_ppid(PathPatternID rhs_ppid) { rhs_ppid_ = rhs_ppid; }

  void set_lhs_vertex_id(size_t lhs_vertex_id) {
    lhs_vertex_id_ = lhs_vertex_id;
  }
  void set_rhs_vertex_id(size_t rhs_vertex_id) {
    rhs_vertex_id_ = rhs_vertex_id;
  }

  bool VariableCompare(MiniCleanCSRGraph* graph,
                       const std::vector<VertexID>& left_path,
                       const std::vector<VertexID>& right_path) {
    VertexAttributeValue lhs_value = graph->GetVertexAttributeValuesByLocalID(
        left_path[lhs_vertex_id_])[lhs_aid_];
    VertexAttributeValue rhs_value = graph->GetVertexAttributeValuesByLocalID(
        right_path[rhs_vertex_id_])[rhs_aid_];
    return Compare(lhs_value, rhs_value);
  }

 private:
  VertexAttributeID lhs_aid_;
  VertexAttributeID rhs_aid_;
  VertexLabel lhs_vlabel_;
  VertexLabel rhs_vlabel_;

  PathPatternID lhs_ppid_;
  PathPatternID rhs_ppid_;
  size_t lhs_vertex_id_;
  size_t rhs_vertex_id_;
};

/* Constant predicate: x.A [op] c */
class ConstantPredicate : public GCRPredicate {
 public:
  ConstantPredicate() = default;
  ConstantPredicate(uint8_t operator_type, VertexLabel lhs_vlabel,
                    VertexAttributeID lhs_aid, VertexAttributeValue c)
      : GCRPredicate(2, operator_type),
        lhs_vlabel_(lhs_vlabel),
        lhs_aid_(lhs_aid),
        c_(c) {}
  ConstantPredicate(const ConstantPredicate& other_constant_predicate)
      : GCRPredicate(other_constant_predicate.get_predicate_type(),
                     other_constant_predicate.get_operator_type()),
        lhs_vlabel_(other_constant_predicate.get_lhs_vlabel()),
        lhs_aid_(other_constant_predicate.get_lhs_aid()),
        c_(other_constant_predicate.get_rhs_value()) {}

  VertexAttributeID get_lhs_vlabel() const { return lhs_vlabel_; }
  VertexAttributeID get_lhs_aid() const { return lhs_aid_; }

  VertexAttributeID get_rhs_value() const { return c_; }

  PathPatternID get_lhs_ppid() const { return lhs_ppid_; }

  size_t get_lhs_vertex_id() const { return lhs_vertex_id_; }

  bool is_in_lhs_pattern() const { return is_in_lhs_pattern_; }

  void set_lhs_ppid(PathPatternID lhs_ppid) { lhs_ppid_ = lhs_ppid; }

  void set_lhs_vertex_id(size_t lhs_vertex_id) {
    lhs_vertex_id_ = lhs_vertex_id;
  }

  void set_is_in_lhs_pattern(bool is_in_lhs_pattern) {
    is_in_lhs_pattern_ = is_in_lhs_pattern;
  }

  bool ConstantCompare(MiniCleanCSRGraph* graph,
                       const std::vector<VertexID>& path) {
    VertexAttributeValue lhs_value = graph->GetVertexAttributeValuesByLocalID(
        path[lhs_vertex_id_])[lhs_aid_];
    return Compare(lhs_value, c_);
  }

 private:
  VertexLabel lhs_vlabel_;
  VertexAttributeID lhs_aid_;
  VertexAttributeValue c_;

  PathPatternID lhs_ppid_;
  size_t lhs_vertex_id_;
  bool is_in_lhs_pattern_;
};

}  // namespace xyz::graph::miniclean::data_structures::gcr

namespace YAML {
template <>
struct convert<xyz::graph::miniclean::data_structures::gcr::VariablePredicate> {
  static Node encode(
      const xyz::graph::miniclean::data_structures::gcr::VariablePredicate&
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
      xyz::graph::miniclean::data_structures::gcr::VariablePredicate&
          variable_predicate) {
    if (node.size() != 5) {
      return false;
    }

    variable_predicate =
        xyz::graph::miniclean::data_structures::gcr::VariablePredicate(
            node["operator_type"].as<uint8_t>(),
            node["lhs_vlabel"]
                .as<xyz::graph::miniclean::common::VertexLabel>(),
            node["lhs_aid"]
                .as<xyz::graph::miniclean::common::VertexAttributeID>(),
            node["rhs_vlabel"]
                .as<xyz::graph::miniclean::common::VertexLabel>(),
            node["rhs_aid"]
                .as<xyz::graph::miniclean::common::VertexAttributeID>());
    return true;
  }
};

template <>
struct convert<xyz::graph::miniclean::data_structures::gcr::ConstantPredicate> {
  static Node encode(
      const xyz::graph::miniclean::data_structures::gcr::ConstantPredicate&
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
      xyz::graph::miniclean::data_structures::gcr::ConstantPredicate&
          constant_predicate) {
    if (node.size() != 4) {
      return false;
    }
    constant_predicate =
        xyz::graph::miniclean::data_structures::gcr::ConstantPredicate(
            node["operator_type"].as<uint8_t>(),
            node["lhs_vlabel"]
                .as<xyz::graph::miniclean::common::VertexLabel>(),
            node["lhs_aid"]
                .as<xyz::graph::miniclean::common::VertexAttributeID>(),
            node["rhs_value"]
                .as<xyz::graph::miniclean::common::VertexAttributeValue>());
    return true;
  }
};

}  // namespace YAML

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_PREDICATE_H_