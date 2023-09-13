#include <gflags/gflags.h>
#include <yaml-cpp/yaml.h>

#include <string>

#include "miniclean/components/preprocessor/index_metadata.h"
#include "miniclean/components/rule_discovery/refactor_rule_miner.h"

using RuleMiner =
    sics::graph::miniclean::components::rule_discovery::refactor::RuleMiner;
using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using MiniCleanCSRGraph =
    sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
using IndexMetadata =
    sics::graph::miniclean::components::preprocessor::IndexMetadata;

DEFINE_string(workspace_path, "", "workspace_path");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Initialize graph.
  YAML::Node metadata = YAML::LoadFile(FLAGS_workspace_path + "/meta.yaml");
  GraphMetadata graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();
  MiniCleanCSRGraph graph(graph_metadata.GetSubgraphMetadata(0));

  RuleMiner rule_miner(&graph);

  LOG_INFO("Loading graph...");
  rule_miner.LoadGraph(FLAGS_workspace_path);
  LOG_INFO("Loading graph done.");

  LOG_INFO("Loading path rules...");
  rule_miner.LoadPathRules(FLAGS_workspace_path);
  LOG_INFO("Loading path rules done.");

  LOG_INFO("Loading path instances...");
  rule_miner.LoadPathInstances(FLAGS_workspace_path + "/matched_path_patterns");
  LOG_INFO("Loading path instances done.");

  LOG_INFO("Loading index metadata...");
  rule_miner.LoadIndexMetadata(FLAGS_workspace_path + "/index_meta.yaml");
  LOG_INFO("Loading index metadata done.");

  LOG_INFO("Init path rules...");
  rule_miner.InitPathRules();
  LOG_INFO("Init path rules done.");

  gflags::ShutDownCommandLineFlags();
}