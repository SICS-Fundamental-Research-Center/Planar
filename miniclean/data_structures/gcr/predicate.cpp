#include "miniclean/data_structures/gcr/predicate.h"

namespace sics::graph::miniclean::data_structures::gcr {

std::string VariablePredicate::GetInfoString() const {
  std::stringstream ss;

  ss << "VariablePredicate: " << std::endl;
  ss << "LHS Label: " << static_cast<int>(lhs_label_) << ", LHS AttrID: "
     << static_cast<int>(lhs_attribute_id_) << std::endl;
  ss << "RHS Label: " << static_cast<int>(rhs_label_) << ", RHS AttrID: "
     << static_cast<int>(rhs_attribute_id_) << std::endl;

  return ss.str();
}

std::string ConcreteVariablePredicate::GetInfoString() const {
  std::stringstream ss;

  ss << "ConcreteVariablePredicate: " << std::endl;
  ss << "Left path index: " << static_cast<int>(left_path_index_)
     << ", Left vertex index: " << static_cast<int>(left_vertex_index_)
     << std::endl;
  ss << "Right path index: " << static_cast<int>(right_path_index_)
     << ", Right vertex index: " << static_cast<int>(right_vertex_index_)
     << std::endl;
  ss << variable_predicate_.GetInfoString();

  return ss.str();
}

bool ConcreteVariablePredicate::TestCompatibility(
    const std::vector<ConcreteVariablePredicate>& lhs_predicates,
    const std::vector<ConcreteVariablePredicate>& rhs_predicates) {
  for (const auto& lhs_predicate : lhs_predicates) {
    uint8_t left_path_index = lhs_predicate.get_left_path_index();
    uint8_t left_vertex_index = lhs_predicate.get_left_vertex_index();
    auto left_attribute_id = lhs_predicate.get_left_attribute_id();
    uint8_t right_path_index = lhs_predicate.get_right_path_index();
    uint8_t right_vertex_index = lhs_predicate.get_right_vertex_index();
    auto right_attribute_id = lhs_predicate.get_right_attribute_id();

    for (const auto& rhs_predicate : rhs_predicates) {
      uint8_t comp_left_path_index = rhs_predicate.get_left_path_index();
      uint8_t comp_left_vertex_index = rhs_predicate.get_left_vertex_index();
      auto comp_left_attribute_id = rhs_predicate.get_left_attribute_id();
      uint8_t comp_right_path_index = rhs_predicate.get_right_path_index();
      uint8_t comp_right_vertex_index = rhs_predicate.get_right_vertex_index();
      auto comp_right_attribute_id = rhs_predicate.get_right_attribute_id();
      if ((left_path_index == comp_left_path_index &&
           left_vertex_index == comp_left_vertex_index &&
           left_attribute_id == comp_left_attribute_id) ||
          (right_path_index == comp_right_path_index &&
           right_vertex_index == comp_right_vertex_index &&
           right_attribute_id == comp_right_attribute_id)) {
        return false;
      }
    }
  }
  return true;
}
}  // namespace sics::graph::miniclean::data_structures::gcr