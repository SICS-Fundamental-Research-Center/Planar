#ifndef MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_CONFIG_H_
#define MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_CONFIG_H_

#include <cstddef>

#include "core/common/types.h"

namespace sics::graph::miniclean::graphs {
  
    struct MiniCleanCSRGraphConfig {
      sics::graph::core::common::VertexID num_vertex;
      sics::graph::core::common::VertexID max_vertex_id;
      size_t num_in_edges;
      size_t num_out_edges;
    };

}

#endif  // MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_CONFIG_H_
