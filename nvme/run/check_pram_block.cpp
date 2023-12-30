#include <gflags/gflags.h>

#include <fstream>

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph_metadata.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_bool(all, false, "whether to check all blocks");
DEFINE_uint32(bid, 0, "block id to check");

using namespace sics::graph;
using sics::graph::core::common::BlockID;
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::VertexID;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string root_path = FLAGS_i;
  auto one_bid = FLAGS_bid;
  auto all_bids = FLAGS_all;

  // read graph of one subgraph
  core::data_structures::GraphMetadata graph_metadata(root_path);
  if (graph_metadata.get_type() != "block") {
    LOG_FATAL("Wrong of type: ", graph_metadata.get_type());
  }

  if (all_bids) {
    // output all block info
    size_t num_blocks = graph_metadata.get_num_subgraphs();
    for (auto i = 0; i < num_blocks; i++) {
      auto subgraph = graph_metadata.GetBlockMetadata(i);
      auto bid = subgraph.bid;
      auto begin_id = subgraph.begin_id;
      auto end_id = subgraph.end_id;
      auto num_vertices = subgraph.num_vertices;
      auto num_edges = subgraph.num_outgoing_edges;
      LOGF_INFO(
          "bid: {}, begin_id: {}, end_id: {}, num_vertices: {}, "
          "num_edges: {}",
          bid, begin_id, end_id, num_vertices, num_edges);
    }
  } else {
    // output one block info
  }
}