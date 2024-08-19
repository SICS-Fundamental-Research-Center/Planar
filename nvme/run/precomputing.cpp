#include <gflags/gflags.h>

#include "nvme/precomputing/neighbor_hop_info.h"
#include "nvme/precomputing/two_hop_neighbor.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_uint32(task_package_factor, 10, "task package factor");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto root_path = FLAGS_i;
  auto parallelism = FLAGS_p;
  auto task_package_factor = FLAGS_task_package_factor;

  sics::graph::nvme::precomputing::ComputeNeighborInfo(
      root_path, task_package_factor, parallelism);
  return 0;
}