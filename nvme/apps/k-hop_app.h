#ifndef GRAPH_SYSTEMS_NVME_APPS_K_HOP_APP_H_
#define GRAPH_SYSTEMS_NVME_APPS_K_HOP_APP_H_

#include <cstdlib>
#include <random>

#include "core/apis/planar_app_base.h"
#include "nvme/apis/block_api.h"
#include "nvme/data_structures/graph/pram_block.h"

namespace sics::graph::nvme::apps {

using BlockGraph = data_structures::graph::BlockCSRGraphUInt32;

class KHopNvmeApp : public apis::BlockModel<BlockGraph::VertexData> {
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexID = core::common::VertexID;

  using FuncVertex = core::common::FuncVertex;
  using FuncEdge = core::common::FuncEdge;
  using FuncEdgeMutate = core::common::FuncEdgeMutate;

 public:
  KHopNvmeApp() = default;
  KHopNvmeApp(const std::string& root_path) : BlockModel(root_path) {
    // Init risk users.
    lambda = core::common::Configurations::Get()->lambda;
    uint32_t num_vertices = GetNumVertices();
    uint32_t num_risk = num_vertices / 0.00001;
    srand(0);
    risk_.Init(num_vertices);
    for (auto i = 0; i < num_risk; i++) {
      auto id = rand() % num_vertices + 1;
      risk_.SetBit(id);
    }
    LOGF_INFO("Original Risky users num: {}", num_risk);
  }
  ~KHopNvmeApp() override = default;

  void Init(VertexID id) { this->Write(id, 0); }

  void SearchOne(VertexID srcId) {
    auto degree = GetOutDegree(srcId);
    if (degree == 0) return;
    auto neighbors = GetEdges(srcId);
    for (VertexID i = 0; i < degree; i++) {
      auto dst = neighbors[i];
      if (risk_.GetBit(dst)) {
        this->WriteAdd(srcId, 1);
      }
    }
  }

  void SearchTwo(VertexID srcId) {
    auto degree = GetOutDegree(srcId);
    if (degree == 0) return;
    auto neighbors = GetEdges(srcId);
    auto num_risk_nei = Read(srcId);
    for (VertexID i = 0; i < degree; i++) {
      auto dst = neighbors[i];
      num_risk_nei += Read(dst);
    }
    if (num_risk_nei > lambda) {
      risk_.SetBit(srcId);
    }
  }

  void Compute() override {
    LOG_INFO("KHopNvmeApp::Compute begin!");
    MapVertex(&init);
    MapVertex(&search_one);
    MapVertex(&search_two);
    auto num = risk_.Count();
    LOG_INFO("Finally, risky users number is: {}", num);
    LOG_INFO("KHopNvmeApp::Compute end!");
  }

 private:
  FuncVertex init = [this](VertexID id) { Init(id); };
  FuncVertex search_one = [this](VertexID id) { SearchOne(id); };
  FuncVertex search_two = [this](VertexID id) { SearchTwo(id); };

  core::common::Bitmap risk_;

  uint32_t lambda = 1;
};

}  // namespace sics::graph::nvme::apps

#endif  // GRAPH_SYSTEMS_NVME_APPS_K_HOP_APP_H_
