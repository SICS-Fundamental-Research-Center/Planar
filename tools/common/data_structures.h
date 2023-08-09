#ifndef TOOLS_COMMON_TYPES_H_
#define TOOLS_COMMON_TYPES_H_

#include <cstdint>
#include <cstddef>

#include "common/types.h"

namespace sics::graph::tools {

struct TMPCSRVertex {
  using VertexID = sics::graph::core::common::VertexID;

  VertexID vid;
  size_t indegree = 0;
  size_t outdegree = 0;
  VertexID* in_edges = nullptr;
  VertexID* out_edges = nullptr;
};

struct EdgelistMetadata {
  using VertexID = sics::graph::core::common::VertexID;
  size_t num_vertices;
  size_t num_edges;
  VertexID max_vid;
};

}  // namespace tools

#endif  // TOOLS_COMMON_TYPES_H_
