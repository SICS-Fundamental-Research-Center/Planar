#include <gflags/gflags.h>

#include "core/apps/wcc_app.h"
#include "core/common/types.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_bool(in_memory, false, "in memory mode");

using namespace sics::graph;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  core::common::Configurations::GetMutable()->root_path = FLAGS_i;
  core::common::Configurations::GetMutable()->parallelism = FLAGS_p;
  core::common::Configurations::GetMutable()->edge_mutate = true;
  core::common::Configurations::GetMutable()->in_memory = FLAGS_in_memory;
  core::common::Configurations::GetMutable()->max_task_package = 50 * FLAGS_p;

  core::planar_system::Planar<core::apps::WCCApp> system(
      core::common::Configurations::Get()->root_path);
  system.Start();
  return 0;
}
