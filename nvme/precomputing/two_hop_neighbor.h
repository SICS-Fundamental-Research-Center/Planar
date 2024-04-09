#ifndef GRAPH_SYSTEMS_NVME_PRECOMPUTING_TWO_HOP_NEIGHBOR_H_
#define GRAPH_SYSTEMS_NVME_PRECOMPUTING_TWO_HOP_NEIGHBOR_H_

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "nvme/precomputing/basic.h"

namespace sics::graph::nvme::precomputing {

// Count the number of two-hop neighbors for each vertex in the graph
void CountHop(const std::string& root_path) {
  // make sure the precomputing directory exists
  std::string precomputing_dir = root_path + "/precomputing";
  if (!fs::exists(precomputing_dir)) {
    if (!fs::create_directories(precomputing_dir)) {
      LOGF_FATAL("Failed creating directory: {}", precomputing_dir.c_str());
    }
  }

  // read GraphMetadata
  core::data_structures::GraphMetadata graph_metadata(root_path);
  auto num_vertices = graph_metadata.get_num_vertices();
  auto num_edges = graph_metadata.get_num_edges();
  auto num_block = graph_metadata.get_num_blocks();

  LOG_INFO("Begin counting two-hop neighbors");
  Blocks blocks(graph_metadata);
  YAML::Node two_hop_info_yaml;
  TwoHopInfos two_hop_infos;
  two_hop_infos.infos.resize(num_block);

  // iter block i to join block j
  for (GraphID i = 0; i < num_block; i++) {
    LOGF_INFO("== Processing block: {} ==", i);
    auto& block_i = blocks.blocks[i];
    block_i.Read(root_path + "blocks/" + std::to_string(i) + ".bin");
    // iter block j for the join of block i
    for (GraphID j = 0; j < num_block; j++) {
      auto& block_j = blocks.blocks[j];
      block_j.Read(root_path + "blocks/" + std::to_string(j) + ".bin");

      auto bid_j = block_j.bid_;
      auto eid_j = block_j.eid_;
      // iter vertex in block i
      for (VertexID k = 0; k < block_i.num_vertices_; k++) {
        auto degree_k = block_i.degree_[k];
        if (degree_k == 0) continue;
        auto one_hop_edges = block_i.GetEdges(k);
        for (VertexID l = 0; l < degree_k; l++) {
          auto hop_1 = one_hop_edges[l];
          if (bid_j <= hop_1 && hop_1 < eid_j) {
            auto degree_1 = block_j.degree_[hop_1 - bid_j];
            if (degree_1 == 0) continue;
            auto two_hop_edges = block_j.GetEdges(hop_1 - bid_j);
            for (int m = 0; m < degree_1; m++) {
              block_i.AddNeighbor(k, two_hop_edges[m]);
            }
          }
        }
      }
    }
    LOG_INFO("Begin delete self loop");
    // del self loop of s -> d -> s
    for (VertexID k = 0; k < block_i.num_vertices_; k++) {
      if (block_i.two_hop_neighbors_[k].find(k + block_i.bid_) !=
          block_i.two_hop_neighbors_[k].end()) {
        block_i.two_hop_neighbors_[k].erase(k + block_i.bid_);
      }
    }
    LOG_INFO("Begin write two-hop info to disk");
    // write two-hop info of block i to disk
    block_i.WriteTwoHopInfo(root_path + "precomputing/" + std::to_string(i) +
                            ".bin");
    two_hop_infos.infos[i].bid = i;
    two_hop_infos.infos[i].num_two_hop_edges = block_i.num_two_hop_edges_;
  }
  LOG_INFO(" ==== ==== ==== ==== ==== ==== ==== ==== ==== ==== ==== ==== ");
  LOG_INFO("Begin write meta info of two-hop neighbors to disk.");
  two_hop_info_yaml["TwoHopInfos"] = two_hop_infos;
  std::ofstream fout(root_path + "precomputing/two_hop_info.yaml");
  fout << two_hop_info_yaml;
  fout.close();
  LOG_INFO("Two-hop neighbors are precomputed.");
}

}  // namespace sics::graph::nvme::precomputing

#endif  // GRAPH_SYSTEMS_NVME_PRECOMPUTING_TWO_HOP_NEIGHBOR_H_
