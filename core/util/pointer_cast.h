#ifndef GRAPH_SYSTEMS_POINTER_CAST_H
#define GRAPH_SYSTEMS_POINTER_CAST_H

#include <memory>

namespace sics::graph::core::util {

// change derived class pointer to base class pointer
template <typename BaseType, typename DerivedType>
inline std::unique_ptr<BaseType> pointer_downcast(
    std::unique_ptr<DerivedType>&& derived) {
  static_assert(std::is_base_of_v<BaseType, DerivedType>,
                "BaseType must be a base class of DerivedType");
  return std::unique_ptr<BaseType>(static_cast<BaseType*>(derived.release()));
}

// change base class pointer to derived class pointer
template <typename BaseType, typename DerivedType>
inline std::unique_ptr<DerivedType> pointer_upcast(
    std::unique_ptr<BaseType>&& based) {
  static_assert(std::is_base_of_v<BaseType, DerivedType>,
                "BaseType must be a base class of DerivedType");
  return std::unique_ptr<DerivedType>(
      dynamic_cast<DerivedType*>(based.release()));
}

}  // namespace sics::graph::core::util

#endif  // GRAPH_SYSTEMS_POINTER_CAST_H
