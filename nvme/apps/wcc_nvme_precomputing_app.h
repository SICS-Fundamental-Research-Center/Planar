#ifndef GRAPH_SYSTEMS_NVME_APPS_WCC_NVME_PRECOMPUTING_APP_H_
#define GRAPH_SYSTEMS_NVME_APPS_WCC_NVME_PRECOMPUTING_APP_H_

#include "core/apis/planar_app_base.h"
#include "nvme/apis/block_api.h"
#include "nvme/data_structures/graph/pram_block.h"

namespace sics::graph::nvme::apps {

using BlockGraph = data_structures::graph::BlockCSRGraphUInt32;

class WCCNvmePreComputingApp : public apis::BlockModel<BlockGraph::VertexData> {
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexID = core::common::VertexID;

  using FuncVertex = core::common::FuncVertex;
  using FuncEdge = core::common::FuncEdge;
  using FuncEdgeAndMutate = core::common::FuncEdgeAndMutate;
  using FuncEdgeMutate = core::common::FuncEdgeMutate;

 public:
  WCCNvmePreComputingApp() = default;
  WCCNvmePreComputingApp(const std::string& root_path)
      : BlockModel(root_path) {}
  ~WCCNvmePreComputingApp() override = default;

  VertexID min(VertexID a, VertexID b) { return a < b ? a : b; }

  void Init(VertexID id) {
    auto min_one_hop = GetMinOneHop(id);
    auto min_two_hop = GetMinTwoHop(id);
    auto root = std::min(min_one_hop, min_two_hop);
    root = std::min(root, id);
    this->Write(id, root);
  }

  void GraftVertex(VertexID src_id) {
    auto degree = GetOutDegree(src_id);
    if (degree == 0) return;
    auto neighbors = GetEdges(src_id);
    VertexID src_parent_id = Read(src_id);
    VertexID tmp = src_parent_id;
    for (int i = 0; i < degree; i++) {
      tmp = min(tmp, Read(neighbors[i]));
    }
    for (int i = 0; i < degree; i++) {
      WriteMin(Read(neighbors[i]), tmp);
    }
    WriteMin(src_parent_id, tmp);
  }

  void Graft(VertexID src_id, VertexID dst_id) {
    VertexID src_parent_id = Read(src_id);
    VertexID dst_parent_id = Read(dst_id);

    if (src_parent_id < dst_parent_id) {
      this->WriteMin(dst_parent_id, src_parent_id);
    } else if (src_parent_id > dst_parent_id) {
      this->WriteMin(src_parent_id, dst_parent_id);
    }
  }

  void PointJump(VertexID src_id) {
    VertexID parent_id = Read(src_id);
    if (parent_id == src_id) {
      return;
    } else {
      while (parent_id != Read(parent_id)) {
        parent_id = Read(parent_id);
      }
      this->WriteMin(src_id, parent_id);
    }
  }

  void Contract(VertexID src_id, VertexID dst_id, EdgeIndex idx) {
    auto src_parent_id = Read(src_id);
    auto dst_parent_id = Read(dst_id);
    if (src_parent_id == dst_parent_id) {
      this->DeleteEdge(src_id, dst_id, idx);
    }
  }

  bool ContractEdge(VertexID src_id, VertexID dst_id) {
    auto src_parent_id = Read(src_id);
    auto dst_parent_id = Read(dst_id);
    if (src_parent_id == dst_parent_id) {
      return true;
    } else {
      return false;
    }
  }

  // TODO:
  // delete pointer 'this' in anonymous namespace
  void Compute() override {
    LOG_INFO("WCCNvmePreComputingApp::Compute() begin");
    int round = 0;
    MapVertex(&init);
    while (true) {
      MapVertex(&graft_vertex);
      MapVertex(&point_jump);
      MapAndMutateEdgeBool(&contractEdge);
      if (update_store_.GetLeftEdges() == 0) {
        LOGF_INFO("======= Round {} end, no edges left =======", round);
        break;
      } else {
        LOGF_INFO("======= Round {} end, left edges num: {} =======", round,
                  update_store_.GetLeftEdges());
      }
      round++;
    }
    LOG_INFO("WCCNvmePreComputingApp::Compute() end");
  }

 private:
  FuncVertex init = [this](VertexID id) { Init(id); };
  FuncVertex graft_vertex = [this](VertexID src_id) { GraftVertex(src_id); };
  FuncEdge graft = [this](VertexID src_id, VertexID dst_id) {
    Graft(src_id, dst_id);
  };
  FuncVertex point_jump = [this](VertexID src_id) { PointJump(src_id); };
  FuncEdgeAndMutate contract = [this](VertexID src_id, VertexID dst_id,
                                      EdgeIndex idx) {
    Contract(src_id, dst_id, idx);
  };
  FuncEdgeMutate contractEdge = [this](VertexID src_id, VertexID dst_id) {
    return ContractEdge(src_id, dst_id);
  };
};

}  // namespace sics::graph::nvme::apps

#endif  // GRAPH_SYSTEMS_NVME_APPS_WCC_NVME_PRECOMPUTING_APP_H_
