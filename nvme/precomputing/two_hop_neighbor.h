#ifndef GRAPH_SYSTEMS_NVME_PRECOMPUTING_TWO_HOP_NEIGHBOR_H_
#define GRAPH_SYSTEMS_NVME_PRECOMPUTING_TWO_HOP_NEIGHBOR_H_

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "nvme/precomputing/basic.h"

namespace sics::graph::nvme::precomputing {

// Count the number of two-hop neighbors for each vertex in the graph
void CountHop(const std::string& root_path, uint32_t parallelism = 1) {
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

  core::common::ThreadPool pool(parallelism);

  for (int i = 0; i < num_block; i++) {
    auto& block = blocks.blocks[i];
    block.Read(root_path + "blocks/" + std::to_string(i) + ".bin");
  }

  // iter block i to join block j
  for (GraphID i = 0; i < num_block; i++) {
    LOGF_INFO("== Processing block: {} ==", i);
    auto& block_i = blocks.blocks[i];
    //    block_i.Read(root_path + "blocks/" + std::to_string(i) + ".bin");

    core::common::TaskPackage tasks;
    // iter block j for the join of block i
    for (GraphID j = 0; j < num_block; j++) {
      LOGF_INFO("== Start join block j {} ==", j);
      auto& block_j = blocks.blocks[j];
      //      block_j.Read(root_path + "blocks/" + std::to_string(j) + ".bin");

      auto bid_j = block_j.bid_;
      auto eid_j = block_j.eid_;

      auto task_size = (block_i.num_vertices_ + parallelism - 1) / parallelism;

      // iter vertex in block i
      VertexID k = 0;
      while (k < block_i.num_vertices_) {
        auto b_k = k;
        auto e_k = std::min(k + task_size, block_i.num_vertices_);
        auto task = [&block_i, &block_j, bid_j, eid_j, b_k, e_k]() {
          for (auto k = b_k; k < e_k; k++) {
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
        };
        tasks.push_back(task);
        k = e_k;
      }
      pool.SubmitSync(tasks);
    }

    //    LOG_INFO("Begin delete self loop");
    // del self loop of s -> d -> s and one hop neighbor
    //    tasks.clear();
    //    VertexID b2 = 0, e2 = 0;
    //    auto task_size = (block_i.num_vertices_ + parallelism - 1) /
    //    parallelism; for (; b2 < block_i.num_vertices_;) {
    //      e2 = std::min(b2 + task_size, block_i.num_vertices_);
    //      auto task = [&block_i, b2, e2]() {
    //        for (auto k = b2; k < e2; k++) {
    //          block_i.two_hop_neighbors_[k].erase(k + block_i.bid_);
    //          auto degree = block_i.degree_[k];
    //          if (degree == 0) continue;
    //          auto edges = block_i.GetEdges(k);
    //          for (VertexID l = 0; l < degree; l++) {
    //            block_i.two_hop_neighbors_[k].erase(edges[l]);
    //          }
    //        }
    //      };
    //      tasks.push_back(task);
    //      b2 = e2;
    //    }
    //    pool.SubmitSync(tasks);
    //    LOG_INFO("Begin write two-hop info to disk");
    // write two-hop info of block i to disk
    //    block_i.WriteTwoHopInfo(
    //        root_path + "precomputing/" + std::to_string(i) + ".bin", &pool);
    //    two_hop_infos.infos[i].bid = i;
    //    two_hop_infos.infos[i].num_two_hop_edges = block_i.num_two_hop_edges_;
  }
  LOG_INFO(" ==== ==== ==== ==== ==== ==== ==== ==== ==== ==== ==== ==== ");
  LOG_INFO("Begin write meta info of two-hop neighbors to disk.");
  two_hop_info_yaml["TwoHopInfos"] = two_hop_infos;
  std::ofstream fout(root_path + "precomputing/two_hop_info.yaml");
  fout << two_hop_info_yaml;
  fout.close();
  LOG_INFO("Two-hop neighbors are precomputed.");
}

// Count the number of two-hop neighbors for each vertex in the graph
void CountHop2(const std::string& root_path, uint32_t parallelism = 1) {
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

  core::common::ThreadPool pool(parallelism);

  for (int i = 0; i < num_block; i++) {
    auto& block = blocks.blocks[i];
    block.Read(root_path + "blocks/" + std::to_string(i) + ".bin");
  }

  // iter block i to join block j
  for (GraphID i = 0; i < num_block; i++) {
    LOGF_INFO("== Processing block: {} ==", i);
    auto& block_i = blocks.blocks.at(i);

    core::common::TaskPackage tasks;
    // iter block j for the join of block i

    auto task_size =
        (block_i.num_vertices_ + parallelism * 10 - 1) / (parallelism * 10);

    // iter vertex in block i
    VertexID k = 0;
    while (k < block_i.num_vertices_) {
      auto b_k = k;
      auto e_k = std::min(k + task_size, block_i.num_vertices_);
      auto task = [&blocks, &block_i, b_k, e_k]() {
        for (auto k = b_k; k < e_k; k++) {
          auto degree_k = block_i.degree_[k];
          if (degree_k == 0) continue;
          auto one_hop_edges = block_i.GetEdges(k);
          for (VertexID l = 0; l < degree_k; l++) {
            auto hop_1 = one_hop_edges[l];
            auto block_j_id = blocks.GetBlockID(hop_1);
            auto& block_j = blocks.blocks.at(block_j_id);
            auto degree_2 = block_j.GetDegreeByID(hop_1);
            if (degree_2 == 0) continue;
            auto two_hop_edges = block_j.GetEdgesByID(hop_1);
            for (int m = 0; m < degree_2; m++) {
              block_i.AddNeighbor(k, two_hop_edges[m]);
            }
          }
        }
      };
      tasks.push_back(task);
      k = e_k;
    }
    pool.SubmitSync(tasks);

    //    LOG_INFO("Begin delete self loop");
    // del self loop of s -> d -> s and one hop neighbor
    //    tasks.clear();
    //    VertexID b2 = 0, e2 = 0;
    //    for (; b2 < block_i.num_vertices_;) {
    //      e2 = std::min(b2 + task_size, block_i.num_vertices_);
    //      auto task = [&block_i, b2, e2]() {
    //        for (auto k = b2; k < e2; k++) {
    //          block_i.two_hop_neighbors_[k].erase(k + block_i.bid_);
    //          auto degree = block_i.GetDegree(k);
    //          if (degree == 0) continue;
    //          auto edges = block_i.GetEdges(k);
    //          for (VertexID l = 0; l < degree; l++) {
    //            block_i.two_hop_neighbors_[k].erase(edges[l]);
    //          }
    //        }
    //      };
    //      tasks.push_back(task);
    //      b2 = e2;
    //    }
    //    pool.SubmitSync(tasks);
    LOG_INFO("Begin write two-hop info to disk");
    // write two-hop info of block i to disk
    //    block_i.WriteTwoHopInfo(
    //        root_path + "precomputing/" + std::to_string(i) + ".bin", &pool);
    //    two_hop_infos.infos[i].bid = i;
    //    two_hop_infos.infos[i].num_two_hop_edges = block_i.num_two_hop_edges_;
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
