#ifndef GRAPH_SYSTEMS_NVME_APPS_SSSP_APP_H_
#define GRAPH_SYSTEMS_NVME_APPS_SSSP_APP_H_

#include "core/apis/planar_app_base.h"
#include "nvme/apis/block_api.h"
#include "nvme/data_structures/graph/pram_block.h"

namespace sics::graph::nvme::apps {

using BlockGraph = data_structures::graph::BlockCSRGraphUInt32;

class SSSPNvmeApp : public apis::BlockModel<BlockGraph::VertexData> {
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexID = core::common::VertexID;

  using FuncVertex = core::common::FuncVertex;
  using FuncEdge = core::common::FuncEdge;
  using FuncEdgeMutate = core::common::FuncEdgeMutate;

 public:
  SSSPNvmeApp() = default;
  SSSPNvmeApp(const std::string& root_path) : BlockModel(root_path) {
    source = core::common::Configurations::Get()->source;
  }
  ~SSSPNvmeApp() override = default;

  void Init(VertexID id) {
    if (id == source) {
      this->Write(id, 0);
    } else {
      this->Write(id, std::numeric_limits<VertexID>::max());
    }
  }

  void Relax(VertexID src_id) {
    auto degree = GetOutDegree(src_id);
    if (degree == 0) return;
    auto edges = GetEdges(src_id);
    auto distance = Read(src_id) + 1;
    for (uint32_t i = 0; i < degree; i++) {
      auto dst = edges[i];
      if (distance < Read(dst)) {
        this->WriteMin(dst, distance);
      }
    }
  }

  void Compute() override {
    LOG_INFO("SSSPNvmeApp::Compute begin!");
    FuncVertex init = [this](VertexID id) { this->Init(id); };
    FuncVertex relax = [this](VertexID src_id) { this->Relax(src_id); };

    MapVertex(&init);
    bool changed = false;
    while (update_store_.IsActive()) {
      MapVertex(&relax);
    }

    LOG_INFO("SSSPNvmeApp::Compute end!");
  }

 private:
  VertexID source = 0;
};

}  // namespace sics::graph::nvme::apps

#endif  // GRAPH_SYSTEMS_NVME_APPS_SSSP_APP_H_
