#include <gflags/gflags.h>

#include "nvme/partition/vertex_equal_block_partition.h"
#include "nvme/precomputing/neighbor_hop_info.h"

DEFINE_string(i, "/testfile", "graph files root path");
// DEFINE_string(o, "/", "output dir for block files");
DEFINE_uint32(p, 8, "parallelism");
DEFINE_string(mode, "vertex", "vertex or edge");
DEFINE_uint32(step_v, 100000, "vertex size for block");
DEFINE_uint32(step_e, 16 * 1024, "edge size for block");
DEFINE_uint32(ratio, 64, "offset contract ratio for CSR format");
DEFINE_uint32(task_package_factor, 10, "task package factor");
DEFINE_bool(precomputing, false, "precomputing mode");
DEFINE_bool(only_pre, false, "only do precomputing");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string root_path = FLAGS_i;
  auto step_v = FLAGS_step_v;
  auto parallelism = FLAGS_p;
  //  auto out_dir = FLAGS_o;
  auto step_e = FLAGS_step_e;
  auto task_package_factor = FLAGS_task_package_factor;
  auto mode = FLAGS_mode;
  auto ratio = FLAGS_ratio;

  if (mode == "vertex") {
    sics::graph::nvme::partition::VertexEqualBlockPartition(root_path, ratio,
                                                            step_v, 0, mode);
  } else {
    sics::graph::nvme::partition::VertexEqualBlockPartition(root_path, ratio, 0,
                                                            step_e, mode);
  }

  if (FLAGS_precomputing) {
    LOG_INFO("\n Precomputing for two hop infos.");
    sics::graph::nvme::precomputing::ComputeNeighborInfo(root_path,
                                                         parallelism);
  }

  return 0;
}
