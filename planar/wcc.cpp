#include <gflags/gflags.h>

#include "core/apps/wcc_app.h"
#include "core/apps/wcc_app_op.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_uint32(task_package_factor, 50, "task package factor");
DEFINE_bool(in_memory, false, "in memory mode");
DEFINE_uint32(memory_size, 64, "memory size (GB)");
DEFINE_uint32(partition, 1,
              "partition type (hashvertex, edgecut, planarcut, 2Dcut)");
DEFINE_uint32(limits, 0, "subgrah limits for pre read");
DEFINE_bool(short_cut, false, "no short cut");
DEFINE_string(buffer_size, "32G", "buffer size for edge blocks");
DEFINE_string(mode, "normal", "mode");

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
  core::common::Configurations::GetMutable()->limits = FLAGS_limits;
  core::common::Configurations::GetMutable()->short_cut = FLAGS_short_cut;
  core::common::Configurations::GetMutable()->edge_buffer_size =
      core::common::GetBufferSize(FLAGS_buffer_size);
  core::common::Configurations::GetMutable()->mode =
      FLAGS_mode == "normal"   ? core::common::Normal
      : FLAGS_mode == "static" ? core::common::Static
                               : core::common::Random;

  LOG_INFO("System begin");
  core::planar_system::Planar<core::apps::WCCAppOp> system(
      core::common::Configurations::Get()->root_path);
  system.Start();

  return 0;
}
