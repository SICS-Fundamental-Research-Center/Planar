#include <gflags/gflags.h>

#include "core/data_structures/graph_metadata.h"
#include "core/util/logging.h"
#include "miniclean/common/types.h"
#include "miniclean/components/rule_discovery/rule_miner.h"
#include "miniclean/data_structures/gcr/predicate.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

using ConstantPredicate =
    sics::graph::miniclean::data_structures::gcr::ConstantPredicate;
using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using MiniCleanCSRGraph =
    sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
using RuleMiner = sics::graph::miniclean::components::rule_discovery::RuleMiner;
using VariablePredicate =
    sics::graph::miniclean::data_structures::gcr::VariablePredicate;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;

DEFINE_string(data_directory, "", "data directory");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Load metadata.
  YAML::Node metadata = YAML::LoadFile(FLAGS_data_directory + "/meta.yaml");
  GraphMetadata graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();

  // Initialize graph.
  MiniCleanCSRGraph graph(graph_metadata.GetSubgraphMetadata(0));

  RuleMiner rule_miner(&graph);

  LOG_INFO("Loading graph...");
  rule_miner.LoadGraph(FLAGS_data_directory);
  LOG_INFO("Loading graph done.");

  LOG_INFO("Loading path patterns...");
  rule_miner.LoadPathPatterns(FLAGS_data_directory + "/path_patterns.txt");
  LOG_INFO("Loading path patterns done.");

  LOG_INFO("Loading path instances...");
  rule_miner.LoadPathInstances(FLAGS_data_directory + "/matched_path_patterns");
  LOG_INFO("Loading path instances done.");

  LOG_INFO("Loading predicates...");
  rule_miner.LoadPredicates(FLAGS_data_directory + "/predicates.yaml");
  LOG_INFO("Loading predicates done.");

  return 0;
}