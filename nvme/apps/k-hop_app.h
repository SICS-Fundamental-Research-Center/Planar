#ifndef GRAPH_SYSTEMS_NVME_APPS_K_HOP_APP_H_
#define GRAPH_SYSTEMS_NVME_APPS_K_HOP_APP_H_

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
  KHopNvmeApp(const std::string& root_path) : BlockModel(root_path) {}
  ~KHopNvmeApp() override = default;

  void Init(VertexID id) { this->Write(id, id); }

  void Search(VertexID srcId) {
    VertexID srcParentId = Read(srcId);
    if (srcParentId == srcId) {
      return;
    } else {
      while (srcParentId != Read(srcParentId)) {
        srcParentId = Read(srcParentId);
      }
      this->WriteMin(srcId, srcParentId);
    }
  }

  void Compute() override {
    LOG_INFO("KHopNvmeApp::Compute begin!");
    FuncVertex init = [this](VertexID id) { this->Init(id); };
    FuncVertex search = [this](VertexID src_id) { this->Search(src_id); };

    MapVertex(&init);
    bool flag = true;
    while (flag) {
      MapVertex(&search);
      if (!update_store_.IsActive()) {
        flag = false;
      }
    }
    LOG_INFO("KHopNvmeApp::Compute end!");
  }
};

}  // namespace sics::graph::nvme::apps

#endif  // GRAPH_SYSTEMS_NVME_APPS_K_HOP_APP_H_
