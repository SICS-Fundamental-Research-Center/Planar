#include <gflags/gflags.h>
#include <yaml-cpp/yaml.h>

#include "core/util/logging.h"
#include "miniclean/components/preprocessor/index_collection.h"

using IndexCollection =
    sics::graph::miniclean::components::preprocessor::IndexCollection;

DEFINE_string(workspace_path, "", "workspace_path");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  IndexCollection index_collection;
  LOG_INFO("Generating index...");
  index_collection.LoadIndexCollection(
      FLAGS_workspace_path + "/attribute_index_config.yaml",
      FLAGS_workspace_path + "/matched_path_patterns",
      FLAGS_workspace_path + "/graph/meta.yaml",
      FLAGS_workspace_path + "/path_patterns.yaml");
  LOG_INFO("Generating index done.");

  gflags::ShutDownCommandLineFlags();

  return 0;
}