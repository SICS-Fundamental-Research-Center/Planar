#include <gflags/gflags.h>
#include <stdlib.h>

#include <chrono>

#include "core/apps/pagerank_app.h"
#include "core/apps/pagerank_app_op.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_uint32(task_package_factor, 50, "task package factor");
DEFINE_bool(in_memory, false, "in memory mode");
DEFINE_uint32(memory_size, 64, "memory size (GB)");
DEFINE_string(buffer_size, "32G", "buffer size for edge blocks");
DEFINE_uint32(limits, 0, "subgrah limits for pre read");
DEFINE_bool(no_short_cut, false, "no short cut");
DEFINE_uint32(iter, 10, "iteration");
DEFINE_bool(radical, false, "radical");
DEFINE_bool(op, true, "optimize version");

using namespace sics::graph;

size_t GetBufferSize(std::string size) {
  size_t res = 0;
  auto num = size.substr(0, size.size() - 1);
  auto unit = size.substr(size.size() - 1);
  if (unit == "G" || unit == "g") {
    res = atoi(num.c_str());
    return res * 1024 * 1024 * 1024;
  } else if (unit == "M" || unit == "m") {
    res = atoi(num.c_str());
    return res * 1024 * 1024;
  } else {
    return 32 * 1024 * 1024 * 1024;
  }
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  core::common::Configurations::GetMutable()->root_path = FLAGS_i;
  core::common::Configurations::GetMutable()->parallelism = FLAGS_p;
  core::common::Configurations::GetMutable()->in_memory = FLAGS_in_memory;
  core::common::Configurations::GetMutable()->task_package_factor =
      FLAGS_task_package_factor;
  core::common::Configurations::GetMutable()->limits = FLAGS_limits;
  core::common::Configurations::GetMutable()->short_cut = !FLAGS_no_short_cut;
  core::common::Configurations::GetMutable()->vertex_data_size = 4;
  core::common::Configurations::GetMutable()->vertex_data_type =
      core::common::VertexDataType::kVertexDataTypeFloat;
  core::common::Configurations::GetMutable()->application =
      core::common::ApplicationType::PageRank;
  core::common::Configurations::GetMutable()->memory_size =
      FLAGS_memory_size * 1024;
  core::common::Configurations::GetMutable()->pr_iter = FLAGS_iter;
  core::common::Configurations::GetMutable()->radical = FLAGS_radical;
  core::common::Configurations::GetMutable()->edge_buffer_size =
      GetBufferSize(FLAGS_buffer_size);

  if (!FLAGS_op) {
    LOG_INFO("System begin");
    core::planar_system::Planar<core::apps::PageRankApp> system(
        core::common::Configurations::Get()->root_path);
    system.Start();
  } else {
    LOG_INFO("Planar System begin");
    core::apps::PageRankOpApp app(FLAGS_i);

    auto begin_time = std::chrono::system_clock::now();

    for (int i = 0; i < FLAGS_iter; i++) {
      if (i == 0) {
        app.PEval();
      } else {
        app.IncEval();
      }
      LOGF_INFO("iter {} finish!", i);
    }

    auto end_time = std::chrono::system_clock::now();
    LOGF_INFO(" =========== whole Runtime: {} s ===========",
              std::chrono::duration<double>(end_time - begin_time).count());
  }
  return 0;
}
