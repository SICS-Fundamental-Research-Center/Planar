#include <gflags/gflags.h>

#include "core/planar_system.h"
#include "nvme/partition/edge_equal_block_partition.h"
#include "nvme/partition/vertex_equal_block_partition.h"
#include "nvme/precomputing/two_hop_neighbor.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_string(o, "/", "output dir for block files");
DEFINE_uint32(p, 8, "parallelism");
DEFINE_uint32(n, 1, "number of partition");
DEFINE_string(mode, "vertex", "vertex or edge");
DEFINE_uint32(step_v, 500000, "vertex step for block");
DEFINE_uint32(step_e, 0, "vertex step for block");
DEFINE_bool(precomputing, false, "precomputing mode");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string root_path = FLAGS_i;
  auto step_v = FLAGS_step_v;
  auto parallelism = FLAGS_p;
  auto out_dir = FLAGS_o;
  auto step_e = FLAGS_step_e;
  auto num_partitions = FLAGS_n;

  if (step_e != 0) {
    LOG_INFO(
        "use step to partition edge, each block has around 'step_e' edges. -n "
        "is no use in this situation");
  } else {
    LOG_INFO("use -n [num] to partition edge. There are at most {} blocks",
             num_partitions);
  }

  if (FLAGS_mode == "edge") {
    // use edge equal block partition
    sics::graph::nvme::partition::EdgeEqualBlockPartition(
        root_path, out_dir, num_partitions, parallelism, step_e);
  } else if (FLAGS_mode == "vertex") {
    sics::graph::nvme::partition::VertexEqualBlockPartition(
        root_path, out_dir, num_partitions, parallelism, step_v);
  }

  if (FLAGS_precomputing) {
    LOG_INFO("\n Precomputing for two hop infos.");
    sics::graph::nvme::precomputing::CountHop(out_dir, parallelism);
  }

  return 0;
}
