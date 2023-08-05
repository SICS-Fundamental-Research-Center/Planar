#ifndef TOOLS_COMMON_TYPES_H_
#define TOOLS_COMMON_TYPES_H_

#include <cstdint>

#include "common/types.h"

namespace tools {

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

// TO DO: Add other data structures.
// template <typename T>
// size_t Hash(T k) {
//  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
//  k = k >> 1;
//  return k;
//}

}  // namespace tools

#endif  // TOOLS_COMMON_TYPES_H_
