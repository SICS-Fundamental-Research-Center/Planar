#ifndef CORE_COMMON_TYPES_H_
#define CORE_COMMON_TYPES_H_

#include <cstdint>

namespace sics::graph::core::common {

typedef uint32_t GraphID;  // uint32_t: 0 ~ 4,294,967,295
typedef uint32_t VertexID;
typedef uint32_t VertexLabel;

#define MAX_VERTEX_ID std::numeric_limits<VertexID>::max()

}  // namespace sics::graph::core::common

#endif  // CORE_COMMON_TYPES_H_
