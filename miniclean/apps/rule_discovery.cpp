#include <gflags/gflags.h>
#include <yaml-cpp/yaml.h>

#include <string>

#include "miniclean/components/rule_discovery/rule_miner.h"

using RuleMiner = sics::graph::miniclean::components::rule_discovery::RuleMiner;
using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using MiniCleanCSRGraph =
    sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;

DEFINE_string(workspace_path, "", "workspace_path");
DEFINE_uint32(parallelism, 1, "number of threads for rule discovery");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Initialize graph.
  YAML::Node metadata =
      YAML::LoadFile(FLAGS_workspace_path + "/graph/meta.yaml");
  GraphMetadata graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();
  MiniCleanCSRGraph graph(graph_metadata.GetSubgraphMetadata(0));

  RuleMiner rule_miner(graph);

  LOG_INFO("Loading graph...");
  rule_miner.LoadGraph(FLAGS_workspace_path);
  LOG_INFO("Loading graph done.");

  LOG_INFO("Loading index collection...");
  rule_miner.LoadIndexCollection(FLAGS_workspace_path);
  LOG_INFO("Loading index collection done.");

  LOG_INFO("Prepare GCR Components...");
  rule_miner.PrepareGCRComponents(FLAGS_workspace_path);
  LOG_INFO("Prepare GCR Components done.");

  LOG_INFO("Start mining...");
  rule_miner.MineGCRsPar(FLAGS_parallelism);
  LOG_INFO("Mining done.");

  gflags::ShutDownCommandLineFlags();
}