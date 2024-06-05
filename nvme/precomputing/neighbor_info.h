#ifndef GRAPH_SYSTEMS_NVME_PRECOMPUTING_NEIGHBOR_INFO_H_
#define GRAPH_SYSTEMS_NVME_PRECOMPUTING_NEIGHBOR_INFO_H_

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "nvme/precomputing/basic.h"

namespace sics::graph::nvme::precomputing {

void ComputeNeighborInfo(const std::string& root_path,
                         uint32_t task_package_factor = 10,
                         uint32_t parallelism = 1) {
  // make sure the precomputing directory exists
  std::string precomputing_dir = root_path + "/precomputing";
  if (!fs::exists(precomputing_dir)) {
    if (!fs::create_directories(precomputing_dir)) {
      LOGF_FATAL("Failed creating directory: {}", precomputing_dir.c_str());
    }
  }

  // read GraphMetadata
  core::data_structures::GraphMetadata graph_metadata(root_path);
  // auto num_vertices = graph_metadata.get_num_vertices();
  // auto num_edges = graph_metadata.get_num_edges();
  auto num_block = graph_metadata.get_num_blocks();

  LOG_INFO("Begin counting two-hop neighbors");
  Blocks blocks(graph_metadata);
  YAML::Node two_hop_info_yaml;
  TwoHopInfos two_hop_infos;
  two_hop_infos.infos.resize(num_block);

  core::common::ThreadPool pool(parallelism);

  for (GraphID i = 0; i < num_block; i++) {
    auto& block = blocks.blocks[i];
    block.Read(root_path + "blocks/" + std::to_string(i) + ".bin");
  }

  // iter block i to join block j
  for (GraphID i = 0; i < num_block; i++) {
    LOGF_INFO("== Processing block: {} ==", i);
    auto& block_i = blocks.blocks.at(i);
    core::common::TaskPackage tasks;
    // iter block j for the join of block i
    auto task_num = parallelism * task_package_factor;
    auto task_size = (block_i.num_vertices + task_num - 1) / task_num;
    LOGF_INFO("task size: {}", task_size);
    // iter vertex in block i
    VertexID k = 0;
    while (k < block_i.num_vertices) {
      auto b_k = k;
      auto e_k = std::min(k + task_size, block_i.num_vertices);
      auto task = [&blocks, &block_i, b_k, e_k]() {
        auto idx = 0;
        for (auto k = b_k; k < e_k; k++) {
          auto degree_k = block_i.degree_[k];
          if (degree_k == 0) continue;
          auto one_hop_edges = block_i.GetEdges(k);
          for (VertexID l = 0; l < degree_k; l++) {
            auto hop_1 = one_hop_edges[l];
            block_i.UpdateOneHopInfo(k, hop_1);
            auto block_j_id = blocks.GetBlockID(hop_1);
            auto& block_j = blocks.blocks.at(block_j_id);
            auto degree_2 = block_j.GetDegreeByID(hop_1);
            if (degree_2 == 0) continue;
            auto two_hop_edges = block_j.GetEdgesByID(hop_1);
            for (VertexDegree m = 0; m < degree_2; m++) {
              block_i.UpdateTwoHopInfo(k, two_hop_edges[m]);
            }
          }
          idx++;
          if (idx % 10000 == 0) {
            LOGF_INFO("{}->{}: Processed {} vertices", b_k, e_k, idx);
          }
        }
      };
      tasks.push_back(task);
      k = e_k;
    }
    LOGF_INFO("Task num: {}", tasks.size());
    pool.SubmitSync(tasks);

    LOG_INFO("Begin write two-hop info to disk");
    // write two-hop info of block i to disk
    block_i.Write(root_path, i, &pool);
  }
  LOG_INFO("Two-hop neighbors are precomputed.");
}

}  // namespace sics::graph::nvme::precomputing

#endif  // GRAPH_SYSTEMS_NVME_PRECOMPUTING_NEIGHBOR_INFO_H_
