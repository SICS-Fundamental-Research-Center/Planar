#ifndef CORE_COMMON_TYPES_H_
#define CORE_COMMON_TYPES_H_

#include <gflags/gflags.h>

#include <cstdint>
#include <limits>
#include <string>

namespace sics::graph::core::common {

typedef uint32_t GraphID;  // uint32_t: 0 ~ 4,294,967,295
typedef uint32_t VertexID;
typedef uint32_t VertexIndex;
typedef uint32_t VertexLabel;
typedef uint32_t VertexCount;
typedef uint64_t EdgeIndex;

typedef uint8_t DefaultEdgeDataType;  // used for position
typedef uint32_t Uint32VertexDataType;
typedef uint16_t Uint16VertexDataType;

#define MAX_VERTEX_ID std::numeric_limits<VertexID>::max()
#define INVALID_GRAPH_ID \
  std::numeric_limits<sics::graph::core::common::GraphID>::max()
#define INVALID_VERTEX_INDEX \
  std::numeric_limits<sics::graph::core::common::VertexIndex>::max()

static int subgraph_limits = 1;  // used only for test.

}  // namespace sics::graph::core::common

#endif  // CORE_COMMON_TYPES_H_
