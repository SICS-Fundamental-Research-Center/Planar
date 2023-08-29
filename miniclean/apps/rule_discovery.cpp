#include "miniclean/components/rule_discovery/rule_miner.h"

#include <gflags/gflags.h>

#include "core/data_structures/graph_metadata.h"
#include "core/util/logging.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using RuleMiner = sics::graph::miniclean::components::rule_discovery::RuleMiner;
using MiniCleanCSRGraph =
    sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;

DEFINE_string(i, "", "data directory");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Load metadata.
  YAML::Node metadata = YAML::LoadFile(FLAGS_i + "/meta.yaml");
  GraphMetadata graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();

  // Initialize graph.
  MiniCleanCSRGraph graph(graph_metadata.GetSubgraphMetadata(0));

  RuleMiner rule_miner(&graph);

  LOG_INFO("Loading path patterns...");
  rule_miner.LoadPathPatterns(FLAGS_i + "/path_patterns.txt");
  LOG_INFO("Loading path patterns done.");

  LOG_INFO("Loading path instances...");
  rule_miner.LoadPathInstances(FLAGS_i + "/matched_path_patterns");
  LOG_INFO("Loading path instances done.");

  return 0;
}