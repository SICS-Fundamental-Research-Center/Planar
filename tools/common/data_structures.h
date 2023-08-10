#ifndef TOOLS_COMMON_TYPES_H_
#define TOOLS_COMMON_TYPES_H_

#include <cstdint>
#include <cstddef>

#include "common/types.h"

namespace sics::graph::tools {

struct EdgelistMetadata {
  using VertexID = sics::graph::core::common::VertexID;
  size_t num_vertices;
  size_t num_edges;
  VertexID max_vid;
};

}  // namespace tools

#endif  // TOOLS_COMMON_TYPES_H_
