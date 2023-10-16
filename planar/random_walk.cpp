#include <gflags/gflags.h>

#include "core/apps/randomwalk_app.h"
#include "core/common/types.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_uint32(task_package_factor, 50, "task package factor");
DEFINE_bool(in_memory, false, "in memory mode");
DEFINE_uint32(memory_size, 64, "memory size (GB)");
DEFINE_uint32(limits, 0, "subgrah limits for pre read");
DEFINE_bool(no_short_cut, false, "no short cut");
DEFINE_uint32(walk, 5, "walk length of random walk");

using namespace xyz::graph;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  core::common::Configurations::GetMutable()->root_path = FLAGS_i;
  core::common::Configurations::GetMutable()->parallelism = FLAGS_p;
  core::common::Configurations::GetMutable()->in_memory = FLAGS_in_memory;
  core::common::Configurations::GetMutable()->task_package_factor =
      FLAGS_task_package_factor;
  core::common::Configurations::GetMutable()->vertex_data_size =
      sizeof(core::apps::RandomWalkApp::VertexData);
  core::common::Configurations::GetMutable()->application =
      core::common::ApplicationType::RandomWalk;
  core::common::Configurations::GetMutable()->memory_size =
      FLAGS_memory_size * 1024;
  core::common::Configurations::GetMutable()->limits = FLAGS_limits;
  core::common::Configurations::GetMutable()->short_cut = !FLAGS_no_short_cut;
  core::common::Configurations::GetMutable()->walk = FLAGS_walk;
  core::common::Configurations::GetMutable()->no_data_need = true;

  LOG_INFO("System begin");
  core::planar_system::Planar<core::apps::RandomWalkApp> system(
      core::common::Configurations::Get()->root_path);
  system.Start();

  return 0;
}
