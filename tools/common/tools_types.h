#ifndef TOOLS_COMMON_TYPES_H_
#define TOOLS_COMMON_TYPES_H_

#include <cstdint>

#include "common/types.h"

using sics::graph::core::common::VertexID;
using sics::graph::core::common::GraphID;

namespace tools::common {

struct SubgraphMetaData {
  GraphID gid;
  size_t num_vertices;
  size_t num_incoming_edges;
  size_t num_outgoing_edges;
  VertexID max_vid;
  VertexID min_vid;
};

// TO DO: Add a structure for the edge list metadata.
struct EdgeListMetaData {
  size_t num_vertices;
  size_t num_edges;
  VertexID max_vid;
  VertexID min_vid;
};

#endif  // TOOLS_COMMON_TYPES_H_
