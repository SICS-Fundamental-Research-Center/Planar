#ifndef CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_CONFIG_H_
#define CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_CONFIG_H_

#include <cstddef>
#include "common/types.h"

namespace xyz::graph::core::data_structures::graph {
    struct ImmutableCSRGraphConfig {
      xyz::graph::core::common::VertexID num_vertex;
      xyz::graph::core::common::VertexID max_vertex;
      size_t sum_in_edges;
      size_t sum_out_edges;
    };
}

#endif  // CORE_DATA_STRUCTURES_GRAPH_IMMUTABLE_CSR_GRAPH_CONFIG_H_
