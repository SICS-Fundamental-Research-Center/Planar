#include "core/common/multithreading/thread_pool.h"

#include <memory>
#include <vector>

#include <gflags/gflags.h>

#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/data_structures/graph_metadata.h"
#include "core/io/basic_reader.h"
#include "core/util/logging.h"
#include "miniclean/components/path_matcher.h"
#include "miniclean/graphs/miniclean_csr_graph.h"

using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using MiniCleanCSRGraph = sics::graph::miniclean::graphs::MiniCleanCSRGraph;
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using BasicReader = sics::graph::core::io::BasicReader;
using VertexID = sics::graph::core::common::VertexID;
using VertexLabel = sics::graph::core::common::VertexLabel;
using ThreadPool = sics::graph::core::common::ThreadPool;
using PathMatcher = sics::graph::miniclean::components::PathMatcher;

DEFINE_string(i, "", "input graph directory");

// @DESCRIPTION: Match path patterns in the graph.
// @PARAMETER: For temporary usage, parameters are hard-coded.
//  - number of vertex labels that will exist in path patterns;
//  - path patterns in `path_patterns` in which each path pattern is represented
//    by a vector of vertex labels;
//  - directory of the input graph when calling `LoadGraph`;
//  - graph configuration in `config`;
int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Load metadata.
  YAML::Node metadata = YAML::LoadFile(FLAGS_i + "/meta.yaml");
  GraphMetadata graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();

  // Initialize graph.
  MiniCleanCSRGraph graph(graph_metadata.GetSubgraphMetadata(0));

  // Initialize label candidates.
  VertexLabel num_label = 4;
  std::set<VertexID> candidates[num_label];

  // Initialize path patterns.
  std::vector<std::vector<VertexID>> path_patterns = {{1, 1}, {1, 2}};

  // Initialize path matcher.
  PathMatcher path_matcher(&graph, path_patterns, candidates, num_label);

  path_matcher.LoadGraph(FLAGS_i);
  path_matcher.BuildCandidateSet(num_label);

  auto start = std::chrono::system_clock::now();
  path_matcher.PathMatching();
  auto end = std::chrono::system_clock::now();

  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count() /
      (double)CLOCKS_PER_SEC;
  
  LOG_INFO("Path matching time: ", duration, " seconds.");

  return 0;
}