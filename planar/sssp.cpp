#include <gflags/gflags.h>

#include "core/apps/sssp_app.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_uint32(task_package_factor, 50, "task package factor");
DEFINE_bool(in_memory, false, "in memory mode");

using namespace sics::graph;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  core::common::Configurations::GetMutable()->root_path = FLAGS_i;
  core::common::Configurations::GetMutable()->parallelism = FLAGS_p;
  core::common::Configurations::GetMutable()->in_memory = FLAGS_in_memory;
  core::common::Configurations::GetMutable()->task_package_factor =
      FLAGS_task_package_factor;
  core::common::Configurations::GetMutable()->vertex_data_size =
      sizeof(core::apps::SsspApp::VertexData);
  core::common::Configurations::GetMutable()->application =
      core::common::ApplicationType::Sssp;

  LOG_INFO("System begin");
  core::planar_system::Planar<core::apps::SsspApp> system(
      core::common::Configurations::Get()->root_path);
  system.Start();
  return 0;
}