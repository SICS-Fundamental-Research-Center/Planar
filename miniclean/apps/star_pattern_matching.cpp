#include "core/common/multithreading/thread_pool.h"

#include <gflags/gflags.h>

#include <memory>
#include <vector>

#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/data_structures/graph_metadata.h"
#include "core/util/logging.h"
#include "miniclean/common/types.h"
#include "miniclean/components/matcher/path_matcher.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using MiniCleanCSRGraph =
    sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
using PathMatcher = sics::graph::miniclean::components::matcher::PathMatcher;
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using ThreadPool = sics::graph::core::common::ThreadPool;
using VertexID = sics::graph::miniclean::common::VertexID;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;

DEFINE_string(i, "", "input graph directory");
DEFINE_uint64(p, 1, "number of threads for path matching");
DEFINE_uint64(t, 1, "number of tasks for path matching");

// @DESCRIPTION: Match path patterns in the graph.
// @PARAMETER:
//  - input graph directory: the directory of the input graph which contains
//      topology, edge label, vertex label, and vertex attributes of each
//      subgraph, the meta data of the graph, and the path patterns.
//      (for testing, the directory is:
//      {PROJECT_ROOT_DIR}/testfile/input/small_graph_path_matching).
//  - parallism: the number of threads for path matching.
//      The default value is 1. The maximum value is the hardware concurrency.
//      User input that less than 1 would adopt 1;
//      User input that greater than the hardware concurrency would adopt the
//      hardware concurrency.
//      The hardware concurrency in sics::50.10 is 20.
//   - num_tasks: the number of tasks for path matching.
//      The default value is 1. It should equal or greater than the parallism.
int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Load metadata.
  YAML::Node metadata = YAML::LoadFile(FLAGS_i + "/meta.yaml");
  GraphMetadata graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();

  // Initialize graph.
  MiniCleanCSRGraph graph(graph_metadata.GetSubgraphMetadata(0));

  // Initialize path matcher.
  PathMatcher path_matcher(&graph);

  path_matcher.LoadGraph(FLAGS_i);
  path_matcher.LoadPatterns(FLAGS_i + "/path_patterns.txt");
  path_matcher.BuildCandidateSet();

  auto start = std::chrono::system_clock::now();
  // the parallelism should greater than 0 and less than the hardware
  // concurrency.
  unsigned int parallelism = FLAGS_p;
  if (parallelism <= 0) {
    LOG_FATAL("The parallelism should greater than 0.");
  }
  parallelism = std::min(parallelism, std::thread::hardware_concurrency());
  // the number of tasks should equal or greater than parallelism.
  unsigned int num_tasks = FLAGS_t;
  if (num_tasks <= 0) {
    LOG_FATAL("The number of tasks should greater than 0.");
  }
  if (num_tasks < parallelism) {
    LOGF_WARN(
        "The number of tasks should equal or greater than parallelism. Set it "
        "to {} instead",
        parallelism);
  }
  num_tasks = parallelism;
  path_matcher.PathMatching(parallelism, num_tasks);
  auto end = std::chrono::system_clock::now();

  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count() /
      (double)CLOCKS_PER_SEC;

  LOG_INFO("Path matching time: ", duration, " seconds.");

  gflags::ShutDownCommandLineFlags();
  return 0;
}