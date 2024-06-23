#include <gflags/gflags.h>

#include "core/apps/mst_app_op.h"
#include "core/common/types.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_uint32(task_package_factor, 50, "task package factor");
DEFINE_bool(in_memory, false, "in memory mode");
DEFINE_uint32(memory_size, 64, "memory size (GB)");
DEFINE_bool(fast, false, "fast mode (no output)");
DEFINE_string(buffer_size, "32G", "buffer size for edge blocks");
DEFINE_bool(short_cut, false, "no short cut");

using namespace sics::graph;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  core::common::Configurations::GetMutable()->root_path = FLAGS_i;
  core::common::Configurations::GetMutable()->parallelism = FLAGS_p;
  core::common::Configurations::GetMutable()->edge_mutate = true;
  core::common::Configurations::GetMutable()->in_memory = FLAGS_in_memory;
  core::common::Configurations::GetMutable()->task_package_factor =
      FLAGS_task_package_factor;
  core::common::Configurations::GetMutable()->application =
      core::common::ApplicationType::MST;
  core::common::Configurations::GetMutable()->memory_size =
      FLAGS_memory_size * 1024;
  core::common::Configurations::GetMutable()->fast = FLAGS_fast;
  core::common::Configurations::GetMutable()->edge_buffer_size =
      core::common::GetBufferSize(FLAGS_buffer_size);
  core::common::Configurations::GetMutable()->short_cut = FLAGS_short_cut;

  LOG_INFO("System begin");
  core::planar_system::Planar<core::apps::MstAppOp> system(
      core::common::Configurations::Get()->root_path);
  system.Start();
  return 0;
}
