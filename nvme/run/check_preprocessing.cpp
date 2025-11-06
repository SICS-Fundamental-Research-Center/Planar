#include <gflags/gflags.h>

#include <fstream>

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph_metadata.h"
#include "nvme/data_structures/graph/block_csr_graph.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_bool(all, false, "whether to check all blocks");
DEFINE_uint32(bid, 0, "block id to check");
DEFINE_bool(two_hop, false, "whether to check two hop neighbors");
DEFINE_bool(paths, false, "whether to check paths");
DEFINE_bool(star4, false, "whether to check star4");
DEFINE_bool(triangles, false, "whether to check triangles");


using namespace sics::graph;
using sics::graph::core::common::BlockID;
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexIndex;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string root_path = FLAGS_i;
  auto one_bid = FLAGS_bid;
  auto all_bids = FLAGS_all;
  auto show_two_hop = FLAGS_two_hop;
  auto show_paths = FLAGS_paths;
  auto show_star4 = FLAGS_star4;
  auto show_triangles = FLAGS_triangles;

  core::data_structures::BlockMetadata meta(root_path);
  nvme::data_structures::graph::BlockCSRGraph graph(root_path, &meta);



}