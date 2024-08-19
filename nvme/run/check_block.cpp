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
DEFINE_bool(csr, false, "check original csr format");

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
  auto csr = FLAGS_csr;

  if (csr) {
    core::data_structures::GraphMetadata graph_metadata(root_path);
    auto subgraph_num = graph_metadata.get_num_subgraphs();
    for (GraphID i = 0; i < subgraph_num; i++) {
      auto subgraph_metadata = graph_metadata.GetSubgraphMetadata(i);
      auto num_vertices = subgraph_metadata.num_vertices;
      auto num_edges = subgraph_metadata.num_outgoing_edges;

      std::ifstream subgraph_file(
          root_path + "graphs/" + std::to_string(i) + ".bin", std::ios::binary);
      if (!subgraph_file) {
        LOG_FATAL("Error opening bin file: ",
                  root_path + "graphs/" + std::to_string(i) + ".bin");
      }
      auto index = new core::common::VertexID[num_vertices];
      auto degree = new core::common::VertexDegree[num_vertices];
      auto offset = new core::common::EdgeIndex[num_vertices];
      auto edges = new core::common::VertexID[num_edges];
      subgraph_file.read((char*)index,
                         num_vertices * sizeof(core::common::VertexID));
      subgraph_file.read((char*)degree,
                         num_vertices * sizeof(core::common::VertexDegree));
      subgraph_file.read((char*)offset,
                         num_vertices * sizeof(core::common::EdgeIndex));
      subgraph_file.read((char*)edges,
                         num_edges * sizeof(core::common::VertexID));

      LOG_INFO("SubgraphID: ", i, ", VertexNum: ", num_vertices,
               ", EdgeNum: ", num_edges);
      for (VertexID idx = 0; idx < num_vertices; idx++) {
        std::string tmp = "Index: " + std::to_string(idx) +
                          ", VertexID: " + std::to_string(index[idx]) + ",";
        tmp += "Degree: " + std::to_string(degree[idx]) + ", ";
        tmp += "Offset: " + std::to_string(offset[idx]) + ", Edges: ";
        auto d = degree[idx];
        auto off = offset[idx];
        for (EdgeIndex j = 0; j < d; j++) {
          tmp += std::to_string(edges[off + j]) + ", ";
        }
        LOG_INFO(tmp);
      }
      delete[] index;
      delete[] degree;
      delete[] offset;
      delete[] edges;
    }
  }

  // Check edges in edges.bin file
  //  std::ifstream data(root_path + "subBlocks/edges.bin", std::ios::binary);
  //  auto edges = new VertexID[meta.num_edges];
  //  data.read((char*)edges, sizeof(VertexID) * meta.num_edges);
  //  for (int i = 0; i < meta.num_edges; i++) {
  //    LOG_INFO("one edges: ", edges[i]);
  //  }

  core::data_structures::BlockMetadata meta(root_path);
  nvme::data_structures::graph::BlockCSRGraph graph(root_path, &meta);

  if (all_bids) {
    for (uint32_t i = 0; i < meta.num_subBlocks; i++) {
      graph.ReadSubBlock(i);
      graph.LogSubBlock(i);
    }
  } else {
    auto bid = one_bid;
    graph.ReadSubBlock(bid);

  }

  // Show two hop neighbors infos

  LOG_INFO(" \n============ Precomputing Infos =============");
  // Read two hop info.
  //  if (show_two_hop) {
  //    size_t num_block = graph_metadata.get_num_blocks();
  //    for (size_t i = 0; i < num_block; i++) {
  //      auto block_metadata = graph_metadata.GetBlockMetadata(i);
  //      // auto bid = block_metadata.bid;
  //      auto begin_id = block_metadata.begin_id;
  //      // auto end_id = block_metadata.end_id;
  //      auto num_vertices = block_metadata.num_vertices;
  //
  //      std::ifstream data_file(
  //          root_path + "precomputing/" + std::to_string(i) +
  //          "_min_one_hop.bin", std::ios::binary);
  //      data_file.seekg(0, std::ios::end);
  //      size_t file_size = data_file.tellg();
  //      data_file.seekg(0, std::ios::beg);
  //      auto data_all = new char[file_size];
  //      data_file.read(data_all, file_size);
  //      VertexID* min_one_hop_addr = (VertexID*)(data_all);
  //
  //      std::ifstream data_file2(
  //          root_path + "precomputing/" + std::to_string(i) +
  //          "_max_one_hop.bin", std::ios::binary);
  //      data_file2.seekg(0, std::ios::end);
  //      size_t file_size2 = data_file2.tellg();
  //      data_file2.seekg(0, std::ios::beg);
  //      auto data_all2 = new char[file_size2];
  //      data_file2.read(data_all2, file_size2);
  //      VertexID* max_one_hop_addr = (VertexID*)(data_all2);
  //
  //      LOG_INFO("One Hop Infos =============");
  //      for (size_t idx = 0; idx < num_vertices; idx++) {
  //        auto id = idx + begin_id;
  //        auto min_one_hop = min_one_hop_addr[idx];
  //        auto max_one_hop = max_one_hop_addr[idx];
  //        LOGF_INFO("id: {}, idx: {}, min: {}, max: {}", id, idx, min_one_hop,
  //                  max_one_hop);
  //      }
  //
  //      LOG_INFO("two Hop Infos =============");
  //      // read the file of two hop
  //      std::ifstream data_file3(
  //          root_path + "precomputing/" + std::to_string(i) +
  //          "_min_two_hop.bin", std::ios::binary);
  //      data_file3.seekg(0, std::ios::end);
  //      size_t file_size3 = data_file3.tellg();
  //      data_file3.seekg(0, std::ios::beg);
  //      auto data_all3 = new char[file_size3];
  //      data_file3.read(data_all3, file_size3);
  //      auto min_two_hop_addr = (VertexID*)(data_all3);
  //
  //      std::ifstream data_file4(
  //          root_path + "precomputing/" + std::to_string(i) +
  //          "_max_two_hop.bin", std::ios::binary);
  //      data_file4.seekg(0, std::ios::end);
  //      size_t file_size4 = data_file4.tellg();
  //      data_file4.seekg(0, std::ios::beg);
  //      auto data_all4 = new char[file_size4];
  //      data_file4.read(data_all4, file_size4);
  //      auto max_two_hop_addr = (VertexID*)(data_all4);
  //      for (VertexIndex idx = 0; idx < num_vertices; idx++) {
  //        auto id = idx + begin_id;
  //        auto min_two_hop = min_two_hop_addr[idx];
  //        auto max_two_hop = max_two_hop_addr[idx];
  //        LOGF_INFO("id: {}, idx: {}, min: {}, max: {}", id, idx, min_two_hop,
  //                  max_two_hop);
  //      }
  //    }
  //  }
}