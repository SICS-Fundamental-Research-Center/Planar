#ifndef GRAPH_SYSTEMS_CORE_APPS_MST_APP_OP_H
#define GRAPH_SYSTEMS_CORE_APPS_MST_APP_OP_H

#include "apis/planar_app_base.h"
#include "apis/planar_app_base_op.h"
#include "common/types.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph/mutable_csr_graph.h"
#include "util/atomic.h"

namespace sics::graph::core::apps {

class MstAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexID = common::VertexID;
  using EdgeIndex = common::EdgeIndex;
  using VertexDegree = common::VertexDegree;

 public:
  MstAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}
  ~MstAppOp() override {
      delete[] min_out_edge_id_;
  };

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub) override {
    apis::PlanarAppBaseOp<uint32_t>::AppInit(runner, meta, buffer, graphs, hub);

    min_out_edge_id_ = new uint32_t[meta->num_vertices];
    for (uint32_t i = 0; i < meta->num_vertices; i++) {
      min_out_edge_id_[i] = MAX_VERTEX_ID;
    }
  }

  void PEval() final {
    LOG_INFO("PEval begins!");
    auto init = [this](VertexID id) { Init(id); };
    auto find_min_edge = [this](VertexID id) { FindMinEdge(id); };
    auto graft = [this](VertexID id) { Graft(id); };
    auto pointer_jump = [this](VertexID id) { PointJump(id); };
    auto contract = [this](VertexID src_id, VertexID dst_id, EdgeIndex idx) {
      Contract(src_id, dst_id, idx);
    };

    ParallelVertexInitDo(init);
    LOG_INFO("init finished!");
    while (GetSubGraphNumEdges() != 0) {
      ParallelVertexDoWithEdges(find_min_edge);
      LOG_INFO("find min edge finished!");

      ParallelAllVertexDo(graft);
      LOG_INFO("graft finished!");

      ParallelAllVertexDo(pointer_jump);
      LOG_INFO("pointer jump finished!");

      ParallelEdgeMutateDo(contract);
      LOGF_INFO("contract finished! left edges: {}", GetSubGraphNumEdges());
    }
  }

  void IncEval() final {
    LOG_INFO("IncEval begins!");
    auto pointer_jump = [this](VertexID id) { PointJump(id); };
    ParallelAllVertexDo(pointer_jump);
    if (mst_active_) {
      mst_active_ = false;
      SetActive();
    } else {
      UnsetActive();
    }
  }
  void Assemble() final {}

 private:
  void Init(VertexID id) { Write(id, id); }

  void FindMinEdge(VertexID id) {
    auto degree = GetOutDegree(id);
    if (degree != 0) {
      auto edges = GetOutEdges(id);
      VertexID min_id = MAX_VERTEX_ID;
      for (VertexDegree i = 0; i < degree; i++) {
        if (IsEdgeDelete(id, i)) continue;
        auto dst = edges[i];
        util::atomic::WriteMin(min_out_edge_id_ + dst, id);
        min_id = dst < min_id ? dst : min_id;
      }
      min_out_edge_id_[id] = min_id;
    }
  }

  void Graft(VertexID src_id) {
    auto dst_id = min_out_edge_id_[src_id];
    if (dst_id != MAX_VERTEX_ID) {
      auto src_parent_id = Read(src_id);
      auto dst_parent_id = Read(dst_id);
      if (src_parent_id < dst_parent_id) {
        WriteMin(dst_parent_id, src_parent_id);
      } else if (src_parent_id > dst_parent_id) {
        WriteMin(src_parent_id, dst_parent_id);
      }
    }
  }

  void PointJump(VertexID src_id) {
    auto parent_id = Read(src_id);
    if (parent_id != Read(parent_id)) {
      while (parent_id != Read(parent_id)) {
        parent_id = Read(parent_id);
      }
      Write(src_id, parent_id);
      mst_active_ = true;
    }
  }

  void Contract(VertexID src_id, VertexID dst_id, EdgeIndex idx) {
    if (Read(src_id) == Read(dst_id)) {
      DeleteEdge(src_id, idx);
    }
  }

  void UpdateMinEdge(VertexID id) { min_out_edge_id_[id] = GetNeiMinId(id); }

 private:
  uint32_t* min_out_edge_id_ = nullptr;
  bool mst_active_ = false;
  // configs
  bool fast_ = false;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_MST_APP_OP_H
