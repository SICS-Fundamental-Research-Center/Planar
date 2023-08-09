#ifndef SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_IO_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_IO_H_

#include <filesystem>
#include <fstream>
#include <string>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "adapter.h"
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

// Adapter for the IO operations.
class IOAdapter {
 public:
  using VertexID = sics::graph::core::common::VertexID;
  using Bitmap = sics::graph::core::common::Bitmap;
  using ImmutableCSRGraph =
      sics::graph::core::data_structures::graph::ImmutableCSRGraph;
  using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
  using SubgraphMetadata = sics::graph::core::data_structures::SubgraphMetadata;
  using EdgelistMetadata = sics::graph::tools::EdgelistMetadata;

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
      std::vector<folly::ConcurrentHashMap<VertexID, TMPCSRVertex>*>&,
      GraphMetadata&, StoreStrategy);

  bool WriteSubgraph(VertexID** edge_bucket, GraphMetadata& graph_metadata,
                     std::vector<EdgelistMetadata> edgelist_metadata_vec,
                     StoreStrategy store_strategy);

 private:
  const std::string output_root_path_;
};

}  // namespace sics::graph::tools

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_IO_H_
