//
// Created by Shuhao Liu on 2023-07-18.
//

#ifndef GRAPH_SYSTEMS_TYPES_H
#define GRAPH_SYSTEMS_TYPES_H

#include <cstdint>

namespace sics::graph::core::common {

#define ALIGNMENT_FACTOR (double)64.0

typedef uint32_t GraphID;  // uint32_t: 0 ~ 4,294,967,295
typedef uint32_t VertexID;

}  // namespace sics::graph::core::common

#endif  // GRAPH_SYSTEMS_TYPES_H
