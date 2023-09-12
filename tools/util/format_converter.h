#ifndef SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_FORMAT_CONVERTER_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_FORMAT_CONVERTER_H_

#include <filesystem>
#include <fstream>
#include <string>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph/immutable_csr_graph.h"
#include "core/data_structures/graph_metadata.h"
#include "core/util/atomic.h"
#include "tools/common/data_structures.h"
#include "tools/common/types.h"
#include "tools/common/yaml_config.h"

namespace sics::graph::tools::util {
namespace format_converter {

// @DESCRIPTION convert edgelist to immutable CSR graph
// @PARAMETERS
//  buffer_edges: base pointer of binary edgelist
//  store_strategy: store strategy for the graph
//  csr_graph: output graph
void Edgelist2CSR(
    const sics::graph::tools::common::Edges& buffer_edges,
    sics::graph::tools::common::StoreStrategy store_strategy,
    sics::graph::core::data_structures::graph::ImmutableCSRGraph* csr_graph);

}  // namespace format_converter
}  // namespace sics::graph::tools::util

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_FORMAT_CONVERTER_H_
