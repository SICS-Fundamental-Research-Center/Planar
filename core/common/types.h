//
// Created by Shuhao Liu on 2023-07-18.
//

#ifndef CORE_COMMON_TYPES_H_
#define CORE_COMMON_TYPES_H_

#include <cstdint>

namespace sics::graph::core::common {

#define ALIGNMENT_FACTOR (double)64.0

typedef uint32_t GraphID;  // uint32_t: 0 ~ 4,294,967,295
typedef uint32_t VertexID;

}  // namespace sics::graph::core::common

#endif  // CORE_COMMON_TYPES_H_
