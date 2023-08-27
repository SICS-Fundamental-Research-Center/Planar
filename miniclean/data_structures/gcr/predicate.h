#ifndef MINICLEAN_DATA_STRUCTURES_GCR_PREDICATE_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_PREDICATE_H_

#include <memory>

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

 protected:
  GCRPredicate(PredicateType predicate_type) : predicate_type(predicate_type) {}

 public:
  PredicateType get_type() const { return predicate_type; }

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

  OperatorType get_operator_type() const { return operator_type_; }

  std::pair<PatternVertexID, VertexAttributeID> get_lhs() const {
    return std::make_pair(lhs_vid_, lhs_aid_);
  }

  std::pair<PatternVertexID, VertexAttributeID> get_rhs() const {
    return std::make_pair(rhs_vid_, rhs_aid_);
  }

 private:
  OperatorType operator_type_;

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

  OperatorType get_operator_type() const { return operator_type_; }

  std::pair<PatternVertexID, VertexAttributeID> get_lhs() const {
    return std::make_pair(lhs_vid_, rhs_aid_);
  }

  ConstantType get_rhs() const { return c_; }

 private:
  OperatorType operator_type_;

  PatternVertexID lhs_vid_;
  VertexAttributeID rhs_aid_;
  ConstantType c_;
};
}  // namespace sics::graph::miniclean::data_structures::gcr

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_PREDICATE_H_