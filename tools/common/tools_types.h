#ifndef TOOLS_COMMON_TYPES_H_
#define TOOLS_COMMON_TYPES_H_

#include <cstdint>

#include "common/types.h"

using sics::graph::core::common::VertexID;
using sics::graph::core::common::GraphID;

namespace tools::common {

struct EdgeListMetaData {
  size_t num_vertices;
  size_t num_edges;
  VertexID max_vid;
  VertexID min_vid;
};

// TO DO: Add other data structures.

} // namespace tools::common

#endif  // TOOLS_COMMON_TYPES_H_
