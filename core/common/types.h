#ifndef CORE_COMMON_TYPES_H_
#define CORE_COMMON_TYPES_H_

#include <gflags/gflags.h>

#include <cstdint>
#include <functional>
#include <limits>
#include <string>

namespace sics::graph::core::common {

typedef uint32_t GraphID;  // uint32_t: 0 ~ 4,294,967,295
typedef uint32_t VertexID;
typedef uint32_t VertexIndex;
typedef uint32_t VertexLabel;
typedef uint32_t VertexDegree;
typedef uint32_t VertexCount;
typedef uint64_t EdgeIndex;
typedef uint32_t BlockID;

typedef uint8_t DefaultEdgeDataType;  // used for position
typedef uint32_t Uint32VertexDataType;
typedef uint16_t Uint16VertexDataType;
typedef float FloatVertexDataType;

typedef std::function<void(VertexID)> FuncVertex;
typedef std::function<void(VertexID, VertexID)> FuncEdge;
typedef std::function<void(VertexID, VertexID, EdgeIndex)> FuncEdgeAndMutate;
typedef std::function<bool(VertexID, VertexID)> FuncEdgeMutate;

#define MAX_VERTEX_ID std::numeric_limits<VertexID>::max()
#define INVALID_GRAPH_ID \
  std::numeric_limits<sics::graph::core::common::GraphID>::max()
#define INVALID_VERTEX_INDEX \
  std::numeric_limits<sics::graph::core::common::VertexIndex>::max()

}  // namespace sics::graph::core::common

#endif  // CORE_COMMON_TYPES_H_
