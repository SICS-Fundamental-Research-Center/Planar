#ifndef CORE_COMMON_TYPES_H_
#define CORE_COMMON_TYPES_H_

#include <cstdint>
#include <limits>

namespace sics::graph::core::common {

typedef uint32_t GraphID;  // uint32_t: 0 ~ 4,294,967,295
typedef uint32_t VertexID;
typedef uint32_t VertexLabel;
typedef uint32_t VertexCount;

#define MAX_VERTEX_ID std::numeric_limits<VertexID>::max()

}  // namespace sics::graph::core::common

// all const defined here
#define INVALID_GRAPH_ID (std::numeric_limits<sics::graph::core::common::GraphID>::max())

#endif  // CORE_COMMON_TYPES_H_
