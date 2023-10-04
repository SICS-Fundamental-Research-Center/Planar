#include <gflags/gflags.h>
#include <yaml-cpp/yaml.h>

#include "core/util/logging.h"
#include "miniclean/components/preprocessor/attribute_bucket.h"

using AttributeBucket =
    sics::graph::miniclean::components::preprocessor::AttributeBucket;

DEFINE_string(workspace_path, "", "workspace_path");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  AttributeBucket path_rule_generator;
  LOG_INFO("Generating path rules...");
  path_rule_generator.InitCategoricalAttributeBucket(
      FLAGS_workspace_path + "/path_patterns.yaml",
      FLAGS_workspace_path + "/attribute/attribute_config.yaml");
  LOG_INFO("Generating path rule done.");

  gflags::ShutDownCommandLineFlags();

  return 0;
}