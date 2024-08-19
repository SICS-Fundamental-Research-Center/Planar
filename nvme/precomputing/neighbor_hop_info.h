#ifndef GRAPH_SYSTEMS_NVME_PRECOMPUTING_NEIGHBOR_HOP_INFO_H_
#define GRAPH_SYSTEMS_NVME_PRECOMPUTING_NEIGHBOR_HOP_INFO_H_

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "nvme/data_structures/neighbor_hop.h"
#include "nvme/precomputing/basic.h"

namespace sics::graph::nvme::precomputing {

void ComputeNeighborInfo(std::string& root_path, uint32_t parallelism = 1) {
  // make sure the precomputing directory exists
  std::string precomputing_dir = root_path + "/precomputing";
  if (!fs::exists(precomputing_dir)) {
    if (!fs::create_directories(precomputing_dir)) {
      LOGF_FATAL("Failed creating directory: {}", precomputing_dir.c_str());
    }
  }

  // read GraphMetadata
  core::data_structures::BlockMetadata metadata(root_path);

  LOG_INFO("Begin counting two-hop neighbors");
  data_structures::graph::BlockCSRGraph graph(root_path, &metadata);
  data_structures::NeighborHopInfo hop_info(root_path, &metadata);
  core::common::ThreadPool pool(parallelism);

  core::common::TaskPackage tasks;
  // iter block j for the join of block i

  auto num_vertices = graph.GetVerticesNum();
  auto task_size = (num_vertices + parallelism - 1) / parallelism;
  LOGF_INFO("task size: {}", task_size);
  // iter vertex in block i
  VertexID k = 0;
  while (k < num_vertices) {
    auto b_k = k;
    auto e_k = std::min(k + task_size, num_vertices);
    auto task = [&graph, &hop_info, b_k, e_k]() {
      auto idx = 0;
      for (auto k = b_k; k < e_k; k++) {
        auto degree_k = graph.GetOutDegree(k);
        if (degree_k == 0) continue;
        auto one_hop_edges = graph.GetOutEdges(k);
        for (VertexID l = 0; l < degree_k; l++) {
          auto hop_1 = one_hop_edges[l];
          hop_info.UpdateOneHopInfo(k, hop_1);
          auto degree_2 = graph.GetOutDegree(hop_1);
          if (degree_2 == 0) continue;
          auto two_hop_edges = graph.GetOutEdges(hop_1);
          for (VertexDegree m = 0; m < degree_2; m++) {
            hop_info.UpdateTwoHopInfo(k, two_hop_edges[m]);
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
  hop_info.SerializeToDisk();

  LOG_INFO("Two-hop neighbors are precomputed.");
}

}  // namespace sics::graph::nvme::precomputing

#endif  // GRAPH_SYSTEMS_NVME_PRECOMPUTING_NEIGHBOR_HOP_INFO_H_
