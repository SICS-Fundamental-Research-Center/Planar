#include <gflags/gflags.h>

#include "nvme/precomputing/two_hop_neighbor.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto root_path = FLAGS_i;
  auto parallelism = FLAGS_p;

  sics::graph::nvme::precomputing::CountHop(root_path);
  return 0;
}