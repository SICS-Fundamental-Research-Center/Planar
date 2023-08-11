#ifndef TOOLS_COMMON_TYPES_H_
#define TOOLS_COMMON_TYPES_H_

#include <cstddef>
#include <cstdint>

#include "core/common/types.h"

namespace sics::graph::tools::common {

struct EdgelistMetadata {
 private:
  using VertexID = sics::graph::core::common::VertexID;

 public:
  size_t num_vertices;
  size_t num_edges;
  VertexID max_vid;
};

}  // namespace sics::graph::tools

#endif  // TOOLS_COMMON_TYPES_H_
