#include <gflags/gflags.h>

#include "core/common/config.h"
#include "core/planar_system.h"
#include "nvme/apps/pagerank_nvme_app.h"
#include "nvme/apps/pagerank_vc_nvme_app.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_uint32(task_package_factor, 50, "task package factor");
DEFINE_bool(in_memory, false, "in memory mode");
DEFINE_uint32(memory_size, 64, "memory size (GB)");
DEFINE_uint32(limits, 0, "subgrah limits for pre read");
DEFINE_bool(short_cut, false, "no short cut");
DEFINE_uint32(task_size, 500000, "task size");
DEFINE_uint32(iter, 10, "pagerank iteration");
DEFINE_bool(pram, false, "pagerank async mode");

using namespace sics::graph;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  core::common::Configurations::GetMutable()->root_path = FLAGS_i;
  core::common::Configurations::GetMutable()->parallelism = FLAGS_p;
  core::common::Configurations::GetMutable()->task_package_factor =
      FLAGS_task_package_factor;
  core::common::Configurations::GetMutable()->in_memory = FLAGS_in_memory;
  core::common::Configurations::GetMutable()->limits = FLAGS_limits;
  core::common::Configurations::GetMutable()->short_cut = FLAGS_short_cut;

  // pagerank nvme specific configurations
  core::common::Configurations::GetMutable()->application =
      core::common::PageRank;
  core::common::Configurations::GetMutable()->is_block_mode = true;
  core::common::Configurations::GetMutable()->task_size = FLAGS_task_size;
  core::common::Configurations::GetMutable()->vertex_data_type =
      core::common::VertexDataType::kVertexDataTypeFloat;
  core::common::Configurations::GetMutable()->pr_iter = FLAGS_iter;

  LOG_INFO("System begin");
  if (!FLAGS_pram) {
    nvme::apps::PageRankVCApp app(FLAGS_i);
    app.Run();
  } else {
    core::common::Configurations::GetMutable()->sync = false;
    nvme::apps::PageRankPullApp app(FLAGS_i);
    app.Run();
  }
  return 0;
}