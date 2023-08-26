#ifndef MINICLEAN_DATA_STRUCTURES_GRAPH_CLEANING_RULES_PREDICATE_H_
#define MINICLEAN_DATA_STRUCTURES_GRAPH_CLEANING_RULES_PREDICATE_H_

#include <memory>

#include "miniclean/common/types.h"

namespace sics::miniclean::data_structures::graph_cleaning_rules {

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

 public:
  GCRPredicate(PredicateType predicate_type) : predicate_type(predicate_type) {}

 public:
  PredicateType predicate_type;
};

/* Variable predicate: x.A [op] y.B */
class VariablePredicate : public GCRPredicate {
 public:
  VariablePredicate(OperatorType operator_type, PatternVertexID lhs_vid,
                    VertexAttributeID lhs_aid, PatternVertexID rhs_vid,
                    VertexAttributeID rhs_aid)
      : GCRPredicate(kVariablePredicate),
        operator_type_(operator_type),
        lhs_vid_(lhs_vid),
        lhs_aid_(lhs_aid),
        rhs_vid_(rhs_vid),
        rhs_aid_(rhs_aid) {}

 public:
  OperatorType operator_type_;

 private:
  PatternVertexID lhs_vid_, rhs_vid_;
  VertexAttributeID lhs_aid_, rhs_aid_;
};

/* Constant predicate: x.A [op] c */
template <typename ConstantType>
class ConstantPredicate : public GCRPredicate {
 public:
  ConstantPredicate(OperatorType operator_type, PatternVertexID lhs_vid,
                    VertexAttributeID rhs_aid, ConstantType c)
      : GCRPredicate(kConstantPredicate),
        operator_type_(operator_type),
        lhs_vid_(lhs_vid),
        rhs_aid_(rhs_aid),
        c_(c) {}

 public:
  OperatorType operator_type_;

 private:
  PatternVertexID lhs_vid_;
  VertexAttributeID rhs_aid_;
  ConstantType c_;
};
}  // namespace sics::miniclean::data_structures::graph_cleaning_rules

#endif  // MINICLEAN_DATA_STRUCTURES_GRAPH_CLEANING_RULES_PREDICATE_H_