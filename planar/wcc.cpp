#include <gflags/gflags.h>

#include "core/apps/wcc_app.h"
#include "core/common/types.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");

using namespace sics::graph;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  core::common::kDefaultRootPath = FLAGS_i;
  core::common::kDefaultParallelism = FLAGS_p;
  // TODO: configure other flags

  core::planar_system::Planar<core::apps::WCCApp> system(
      core::common::kDefaultRootPath);
  system.Start();
  return 0;
}
