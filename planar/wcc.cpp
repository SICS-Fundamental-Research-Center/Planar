#include <gflags/gflags.h>

#include "core/apps/wcc_app.h"
#include "core/apps/wcc_edgecut_app.h"
#include "core/common/types.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_uint32(task_package_factor, 50, "task package factor");
DEFINE_bool(in_memory, false, "in memory mode");
DEFINE_uint32(memory_size, 64, "memory size (GB)");
DEFINE_uint32(partition, 1,
              "partition type (hashvertex, edgecut, planarcut, 2Dcut)");

using namespace sics::graph;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  core::common::Configurations::GetMutable()->root_path = FLAGS_i;
  core::common::Configurations::GetMutable()->parallelism = FLAGS_p;
  core::common::Configurations::GetMutable()->edge_mutate = true;
  core::common::Configurations::GetMutable()->in_memory = FLAGS_in_memory;
  core::common::Configurations::GetMutable()->task_package_factor =
      FLAGS_task_package_factor;
  core::common::Configurations::GetMutable()->vertex_data_size =
      sizeof(core::apps::WCCApp::VertexData);
  core::common::Configurations::GetMutable()->application =
      core::common::ApplicationType::WCC;
  core::common::Configurations::GetMutable()->memory_size =
      FLAGS_memory_size * 1024;

  if (FLAGS_partition == core::common::PartitionType::EdgeCut) {
    core::common::Configurations::GetMutable()->partition_type =
        core::common::PartitionType::EdgeCut;
    LOG_INFO("System begin, EdgeCut WCC");
    core::planar_system::Planar<core::apps::WCCEdgeCutApp> system(
        core::common::Configurations::Get()->root_path);
    system.Start();
  } else {
    LOG_INFO("System begin");
    core::planar_system::Planar<core::apps::WCCApp> system(
        core::common::Configurations::Get()->root_path);
    system.Start();
  }
  return 0;
}
