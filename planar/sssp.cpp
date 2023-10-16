#include <gflags/gflags.h>

#include "core/apps/sssp_app.h"
#include "core/apps/sssp_asp_app.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_uint32(task_package_factor, 50, "task package factor");
DEFINE_bool(in_memory, false, "in memory mode");
DEFINE_uint32(memory_size, 64, "memory size (GB)");
DEFINE_uint32(source, 0, "source vertex id");
DEFINE_bool(ASP, false, "debug mode");

using namespace xyz::graph;

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
  core::common::Configurations::GetMutable()->memory_size =
      FLAGS_memory_size * 1024;
  core::common::Configurations::GetMutable()->source = FLAGS_source;
  core::common::Configurations::GetMutable()->ASP = FLAGS_ASP;

  if (FLAGS_ASP) {
    LOG_INFO("use asp sssp");
    LOG_INFO("System begin");
    core::planar_system::Planar<core::apps::SsspAspApp> system(
        core::common::Configurations::Get()->root_path);
    system.Start();
  } else {
    LOG_INFO("System begin");
    core::planar_system::Planar<core::apps::SsspApp> system(
        core::common::Configurations::Get()->root_path);
    system.Start();
  }
  return 0;
}
