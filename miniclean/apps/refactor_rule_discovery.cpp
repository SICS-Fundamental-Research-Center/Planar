#include <gflags/gflags.h>
#include <string>

#include "miniclean/components/rule_discovery/refactor_rule_miner.h"

using RuleMiner =
    sics::graph::miniclean::components::rule_discovery::refactor::RuleMiner;

DEFINE_string(workspace_path, "", "workspace_path");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  RuleMiner rule_miner;
  rule_miner.LoadPathRules(FLAGS_workspace_path);

  gflags::ShutDownCommandLineFlags();
}