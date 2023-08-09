#ifndef SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_ADAPTER_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_ADAPTER_H_

#include <filesystem>
#include <fstream>
#include <string>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "common/bitmap.h"
#include "common/multithreading/thread_pool.h"
#include "common/types.h"
#include "data_structures.h"
#include "data_structures/graph/immutable_csr_graph.h"
#include "data_structures/graph_metadata.h"
#include "types.h"
#include "util/atomic.h"
#include "yaml_config.h"

namespace sics::graph::tools {

class GraphAdapter {
  using VertexID = sics::graph::core::common::VertexID;
  using Bitmap = sics::graph::core::common::Bitmap;
  using ImmutableCSRGraph =
      sics::graph::core::data_structures::graph::ImmutableCSRGraph;

 public:
  GraphAdapter() = default;

  void Edgelist2CSR(VertexID*, EdgelistMetadata, ImmutableCSRGraph&,
                    const StoreStrategy);
};

}  // namespace sics::graph::tools

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_ADAPTER_H_
