#include <gflags/gflags.h>

#include "nvme/precomputing/neighbor_hop_info.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_uint32(task_package_factor, 100, "task package factor");
DEFINE_bool(neibor, false, "precompute the neighbor info");
DEFINE_bool(pattern, false, "precompute the customized pattern");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto root_path = FLAGS_i;
  auto parallelism = FLAGS_p;
  auto task_package_factor = FLAGS_task_package_factor;
  auto pattern = FLAGS_pattern;
  auto neighbor = FLAGS_neibor;

  if (neighbor) {
    LOG_INFO("====== Compute neighbor infos ====== ");
    sics::graph::nvme::precomputing::ComputeNeighborInfo(root_path, parallelism);
  }

  if (pattern) {
    LOG_INFO("====== Compute pattern infos ====== ");
    sics::graph::nvme::precomputing::PatternIndex(root_path, parallelism, task_package_factor);
  }

  return 0;
}