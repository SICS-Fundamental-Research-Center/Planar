#include <gflags/gflags.h>

#include "core/apps/wcc_app.h"
#include "core/common/types.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");

using namespace sics::graph;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  core::common::configs.root_path = FLAGS_i;
  core::common::configs.parallelism = FLAGS_p;
  core::common::configs.edge_mutate = true;
  // TODO: configure other flags

  core::planar_system::Planar<core::apps::WCCApp> system(
      core::common::configs.root_path);
  system.Start();
  return 0;
}
