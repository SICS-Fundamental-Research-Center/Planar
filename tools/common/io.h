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
class GraphFormatConverter {
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
  GraphFormatConverter(const std::string& output_root_path)
      : output_root_path_(output_root_path) {
    if (!std::filesystem::exists(output_root_path))
      std::filesystem::create_directory(output_root_path);
    if (!std::filesystem::exists(output_root_path + "/label"))
      std::filesystem::create_directory(output_root_path + "/label");
    if (!std::filesystem::exists(output_root_path + "/graphs"))
      std::filesystem::create_directory(output_root_path + "/graphs");
  }

  // @DESCRIPTION Write a set of vertices to disk. It first merges vertices to
  // construct CSR graphs, one CSR for each ConcurrentHashMap,and then store the
  // CSR graphs in output_root_path/graphs.
  // @PARAMETERS
  //  subgraph_vec: a vector of vertices to be written to disk.
  //  graph_metadata: metadata of the graph.
  //  store_strategy: store strategy to be used.
  void WriteSubgraph(
      const std::vector<folly::ConcurrentHashMap<VertexID, Vertex>*>&
          subgraph_vec,
      const GraphMetadata& graph_metadata, StoreStrategy store_strategy);

  // @DESCRIPTION Write a set of edges to disk. It first converts edges to CSR
  // graphs, one CSR for each bucket, and then store the CSR graphs in
  // output_root_path/graphs.
  // @PARAMETERS
  //  edge_bucket: a set of edges.
  //  graph_metadata: metadata of the graph.
  //  edgelist_metadata_vec: metadata for each bucket.
  //  store_strategy: store strategy to be used.
  void WriteSubgraph(VertexID** edge_bucket,
                     const GraphMetadata& graph_metadata,
                     const std::vector<EdgelistMetadata>& edgelist_metadata_vec,
                     StoreStrategy store_strategy);

 private:
  const std::string output_root_path_;
};

}  // namespace sics::graph::tools::common

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_IO_H_
