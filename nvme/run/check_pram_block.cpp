#include <gflags/gflags.h>

#include <fstream>

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph_metadata.h"
#include "core/planar_system.h"
#include "nvme/precomputing/basic.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_bool(all, false, "whether to check all blocks");
DEFINE_uint32(bid, 0, "block id to check");
DEFINE_bool(two_hop, false, "whether to check two hop neighbors");

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
  auto show_two_hop = FLAGS_two_hop;

  // read graph of one subgraph
  core::data_structures::GraphMetadata graph_metadata(root_path);
  if (graph_metadata.get_type() != "block") {
    LOG_FATAL("Wrong of type: ", graph_metadata.get_type());
  }

  if (all_bids) {
    // output all block info
    size_t num_blocks = graph_metadata.get_num_subgraphs();
    for (auto i = 0; i < num_blocks; i++) {
      auto block_metadata = graph_metadata.GetBlockMetadata(i);
      auto bid = block_metadata.bid;
      auto begin_id = block_metadata.begin_id;
      auto end_id = block_metadata.end_id;
      auto num_vertices = block_metadata.num_vertices;
      auto num_edges = block_metadata.num_outgoing_edges;
      LOGF_INFO(
          "bid: {}, begin_id: {}, end_id: {}, num_vertices: {}, "
          "num_edges: {}",
          bid, begin_id, end_id, num_vertices, num_edges);

      // read the file
      std::ifstream data_file(
          root_path + "blocks/" + std::to_string(bid) + ".bin",
          std::ios::binary);
      if (!data_file) {
        LOG_FATAL("Error opening bin file: ",
                  root_path + "blocks/" + std::to_string(bid) + ".bin");
      }
      data_file.seekg(0, std::ios::end);
      size_t file_size = data_file.tellg();
      data_file.seekg(0, std::ios::beg);
      auto data_all = new char[file_size];
      data_file.read(data_all, file_size);

      size_t size_degree = num_vertices * sizeof(VertexID);
      size_t size_offset = num_vertices * sizeof(EdgeIndex);

      VertexID* degree_addr = (VertexID*)(data_all);
      EdgeIndex* offset_addr = (EdgeIndex*)(data_all + size_degree);
      VertexID* edge_addr = (VertexID*)(data_all + size_degree + size_offset);

      for (size_t idx = 0; idx < num_vertices; idx++) {
        auto id = idx + begin_id;
        auto degree = degree_addr[idx];
        auto offset = offset_addr[idx];
        VertexID* addr = edge_addr + offset;
        std::string edges_str = "";
        for (size_t i = 0; i < degree; i++) {
          auto neighbor_id = addr[i];
          edges_str += std::to_string(neighbor_id) + " ";
        }
        LOGF_INFO("id: {}, idx: {}, degree: {}, offset: {} ==> neighbor_id: {}",
                  id, idx, degree, offset, edges_str);
      }
    }
  } else {
    // output one block info
  }

  // Show two hop neighbors infos

  LOG_INFO(" \n============ Precomputing Infos =============");
  // Read two hop info.
  if (show_two_hop) {
    size_t num_block = graph_metadata.get_num_blocks();
    for (auto i = 0; i < num_block; i++) {
      auto block_metadata = graph_metadata.GetBlockMetadata(i);
      auto bid = block_metadata.bid;
      auto begin_id = block_metadata.begin_id;
      auto end_id = block_metadata.end_id;
      auto num_vertices = block_metadata.num_vertices;

      std::ifstream data_file(root_path + "precomputing/one_hop_min.bin",
                              std::ios::binary);
      data_file.seekg(0, std::ios::end);
      size_t file_size = data_file.tellg();
      data_file.seekg(0, std::ios::beg);
      auto data_all = new char[file_size];
      data_file.read(data_all, file_size);
      VertexID* min_one_hop_addr = (VertexID*)(data_all);

      std::ifstream data_file2(root_path + "precomputing/one_hop_max.bin",
                               std::ios::binary);
      data_file2.seekg(0, std::ios::end);
      size_t file_size2 = data_file2.tellg();
      data_file2.seekg(0, std::ios::beg);
      auto data_all2 = new char[file_size2];
      data_file2.read(data_all2, file_size2);
      VertexID* max_one_hop_addr = (VertexID*)(data_all2);

      LOG_INFO("One Hop Infos =============");
      for (size_t idx = 0; idx < num_vertices; idx++) {
        auto id = idx + begin_id;
        auto min_one_hop = min_one_hop_addr[idx];
        auto max_one_hop = max_one_hop_addr[idx];
        LOGF_INFO("id: {}, idx: {}, min: {}, max: {}", id, idx, min_one_hop,
                  max_one_hop);
      }

      LOG_INFO("two Hop Infos =============");
      // read the file of two hop
      std::ifstream data_file3(root_path + "precomputing/two_hop_min.bin",
                               std::ios::binary);
      data_file3.seekg(0, std::ios::end);
      size_t file_size3 = data_file3.tellg();
      data_file3.seekg(0, std::ios::beg);
      auto data_all3 = new char[file_size3];
      data_file3.read(data_all3, file_size3);
      auto min_two_hop_addr = (VertexID*)(data_all3);

      std::ifstream data_file4(root_path + "precomputing/two_hop_max.bin",
                               std::ios::binary);
      data_file4.seekg(0, std::ios::end);
      size_t file_size4 = data_file4.tellg();
      data_file4.seekg(0, std::ios::beg);
      auto data_all4 = new char[file_size4];
      data_file4.read(data_all4, file_size4);
      auto max_two_hop_addr = (VertexID*)(data_all4);
      for (auto idx = 0; idx < num_vertices; idx++) {
        auto id = idx + begin_id;
        auto min_two_hop = min_two_hop_addr[idx];
        auto max_two_hop = max_two_hop_addr[idx];
        LOGF_INFO("id: {}, idx: {}, min: {}, max: {}", id, idx, min_two_hop,
                  max_two_hop);
      }
    }
  }
}