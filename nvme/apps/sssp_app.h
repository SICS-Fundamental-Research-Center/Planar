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
  SSSPNvmeApp(const std::string& root_path) : BlockModel(root_path) {}
  ~SSSPNvmeApp() override = default;

  void Init(VertexID id) {
    this->Write(id, std::numeric_limits<VertexID>::max());
  }

  void Relax(VertexID src_id, VertexID dst_id) {
    VertexID src_dist = Read(src_id);
    VertexID dst_dist = Read(dst_id);
    if (src_dist + 1 < dst_dist) {
      this->Write(dst_id, src_dist + 1);
    }
  }

  void RelaxEdge(VertexID src_id, VertexID dst_id) {
    VertexID src_dist = Read(src_id);
    VertexID dst_dist = Read(dst_id);
    if (src_dist + 1 < dst_dist) {
    }
  }

  void Compute() override {
    LOG_INFO("SSSPNvmeApp::Compute begin!");
    FuncVertex init = [this](VertexID id) { this->Init(id); };
    FuncEdge relax = [this](VertexID src_id, VertexID dst_id) {
      this->Relax(src_id, dst_id);
    };
    FuncEdge relax_edge = [this](VertexID src_id, VertexID dst_id) {
      this->RelaxEdge(src_id, dst_id);
    };

    MapVertex(&init);
    bool changed = false;
    while (true) {
      MapEdge(&relax);
      MapEdge(&relax_edge);
      if (!changed) {
        break;
      }
    }

    LOG_INFO("SSSPNvmeApp::Compute end!");
  }
};

}  // namespace sics::graph::nvme::apps

#endif  // GRAPH_SYSTEMS_NVME_APPS_SSSP_APP_H_
