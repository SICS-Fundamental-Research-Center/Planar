#include <gflags/gflags.h>

#include "core/apps/clique_five_app.h"
#include "core/apps/clique_four_app.h"
#include "core/apps/path_count.h"
#include "core/apps/triangle_app.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_bool(in_memory, true, "in memory mode");
DEFINE_uint32(memory_size, 64, "memory size (GB)");
DEFINE_uint32(partition, 1,
              "partition type (hashvertex, edgecut, planarcut, 2Dcut)");
DEFINE_string(buffer_size, "32G", "buffer size for edge blocks");
DEFINE_string(mode, "normal", "mode");
DEFINE_string(query, "3-clique", "default query structure");
DEFINE_uint32(length, 3, "path length for query");

using namespace sics::graph;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  core::common::Configurations::GetMutable()->root_path = FLAGS_i;
  core::common::Configurations::GetMutable()->parallelism = FLAGS_p;
  core::common::Configurations::GetMutable()->edge_mutate = true;
  core::common::Configurations::GetMutable()->in_memory = FLAGS_in_memory;
  core::common::Configurations::GetMutable()->application =
      core::common::ApplicationType::Query;
  core::common::Configurations::GetMutable()->memory_size =
      FLAGS_memory_size * 1024;
  core::common::Configurations::GetMutable()->edge_buffer_size =
      core::common::GetBufferSize(FLAGS_buffer_size);
  core::common::Configurations::GetMutable()->mode =
      FLAGS_mode == "normal"   ? core::common::Normal
      : FLAGS_mode == "static" ? core::common::Static
                               : core::common::Random;

  core::common::Configurations::GetMutable()->path_length = FLAGS_length;
  LOG_INFO("System begin");

  if (FLAGS_query == "path") {
    core::planar_system::Planar<core::apps::PathCountAppOp> system(
        core::common::Configurations::Get()->root_path);
    system.Start();
  } else if (FLAGS_query == "star") {
    core::planar_system::Planar<core::apps::StarAppOp> system(
        core::common::Configurations::Get()->root_path);
    system.Start();
  }
  return 0;
}
