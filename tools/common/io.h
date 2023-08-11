#ifndef SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_IO_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_IO_H_

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
#include "tools/util/format_converter.h"

namespace sics::graph::tools::common {

// Adapter for the IO operations.
class IOAdapter {
 private:
  using VertexID = sics::graph::core::common::VertexID;
  using Bitmap = sics::graph::core::common::Bitmap;
  using ImmutableCSRGraph =
      sics::graph::core::data_structures::graph::ImmutableCSRGraph;
  using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
  using SubgraphMetadata = sics::graph::core::data_structures::SubgraphMetadata;
  using EdgelistMetadata = sics::graph::tools::common::EdgelistMetadata;
  using Vertex = sics::graph::core::data_structures::graph::ImmutableCSRVertex;

 public:
  IOAdapter(const std::string& output_root_path)
      : output_root_path_(output_root_path) {
    if (!std::filesystem::exists(output_root_path))
      std::filesystem::create_directory(output_root_path);
    if (!std::filesystem::exists(output_root_path))
      std::filesystem::create_directory(output_root_path);
    if (!std::filesystem::exists(output_root_path + "/label"))
      std::filesystem::create_directory(output_root_path + "/label");
    if (!std::filesystem::exists(output_root_path + "/graphs"))
      std::filesystem::create_directory(output_root_path + "/graphs");
  }

  bool WriteSubgraph(
      const std::vector<folly::ConcurrentHashMap<VertexID, Vertex>*>&
          subgraph_vec,
      const GraphMetadata& graph_metadata, StoreStrategy store_strategy);

  bool WriteSubgraph(VertexID** edge_bucket,
                     const GraphMetadata& graph_metadata,
                     const std::vector<EdgelistMetadata>& edgelist_metadata_vec,
                     StoreStrategy store_strategy);

 private:
  const std::string output_root_path_;
};

}  // namespace sics::graph::tools::common

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_IO_H_
