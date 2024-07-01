#ifndef GRAPH_SYSTEMS_CORE_APPS_WCC_APP_OP_H
#define GRAPH_SYSTEMS_CORE_APPS_WCC_APP_OP_H

#include <unordered_map>

#include "apis/planar_app_base_op.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

class WCCAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

 public:
  WCCAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}
  ~WCCAppOp() override = default;

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub, scheduler::GraphState* state) override {
    apis::PlanarAppBaseOp<uint32_t>::AppInit(runner, meta, buffer, graphs, hub,
                                             state);
  }

  void PEval() final {
    auto init = [this](VertexID id) { Init(id); };
    auto graft = [this](VertexID src_id, VertexID dst_id) {
      Graft(src_id, dst_id);
    };
    auto pointer_jump = [this](VertexID id) { PointJump(id); };
    auto contract = [this](VertexID src_id, VertexID dst_id, EdgeIndex idx) {
      Contract(src_id, dst_id, idx);
    };
    auto contract_vertex = [this](VertexID id) { ContractVertex(id); };

    ParallelVertexInitDo(init);
    //    LogVertexState();

    LOG_INFO("PEval begins");
    auto size = GetSubGraphNumEdges();
    while (size != 0) {
      LOGF_INFO("Edges num: {}", size);
      ParallelEdgeMutateDo(graft);
      LOG_INFO("Graft finishes");
      //      LogCurrentGraphInfo();
      // LogVertexState();
      ParallelAllVertexDo(pointer_jump);
      LOG_INFO("Pointer jump finishes");
      //      LogVertexState();
      //      ParallelEdgeMutateDo(contract);
      ParallelVertexDoWithEdges(contract_vertex);
      size = GetSubGraphNumEdges();
      LOGF_INFO("Contract finishes! left edges: {}", size);
      //      LogCurrentGraphVertexState();
      //      LogCurrentGraphDelInfo();
    }
    LOG_INFO("PEval finishes");
  }

  void IncEval() final {
    auto pointer_jump = [this](VertexID id) { PointJump(id); };
    LOG_INFO("IncEval begins");
    ParallelAllVertexDo(pointer_jump);
    LOG_INFO("IncEval finishes");
  }

  void Assemble() final {}

 private:
  void Init(VertexID id) { Write(id, id); }

  void Graft(VertexID src_id, VertexID dst_id) {
    auto src_parent_id = Read(src_id);
    auto dst_parent_id = Read(dst_id);
    if (src_parent_id < dst_parent_id) {
      WriteMin(dst_parent_id, src_parent_id);
    } else if (src_parent_id > dst_parent_id) {
      WriteMin(src_parent_id, dst_parent_id);
    }
  }

  void PointJump(VertexID src_id) {
    auto prev_parent_id = Read(src_id);
    auto parent_id = prev_parent_id;
    if (parent_id != Read(parent_id)) {
      while (parent_id != Read(parent_id)) {
        parent_id = Read(parent_id);
      }
    }
    if (prev_parent_id != parent_id) {
      WriteActive(src_id, parent_id);
    }
  }

  void Contract(VertexID src_id, VertexID dst_id, EdgeIndex idx) {
    if (Read(src_id) == Read(dst_id)) {
      DeleteEdge(src_id, idx);
    }
  }

  void ContractVertex(VertexID id) {
    auto degree = GetOutDegree(id);
    if (degree != 0) {
      auto edges = GetOutEdges(id);
      auto src_parent = Read(id);
      for (VertexDegree i = 0; i < degree; i++) {
        if (IsEdgeDelete(id, i)) continue;
        auto dst = edges[i];
        if (src_parent == Read(dst)) {
          DeleteEdgeByVertex(id, i);
          //          LOGF_INFO("del src {}->dst {} idx {}", id, dst, i);
        }
      }
    }
  }

  void PointJumpIncEval(VertexID id);

 private:
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_WCC_APP_OP_H
