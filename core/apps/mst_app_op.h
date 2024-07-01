#ifndef GRAPH_SYSTEMS_CORE_APPS_MST_APP_OP_H
#define GRAPH_SYSTEMS_CORE_APPS_MST_APP_OP_H

#include <mutex>

#include "apis/planar_app_base.h"
#include "apis/planar_app_base_op.h"
#include "common/types.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph/mutable_csr_graph.h"
#include "util/atomic.h"

namespace sics::graph::core::apps {

#define MST_INVALID_ID 0xffffffff

class MstAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexID = common::VertexID;
  using EdgeIndex = common::EdgeIndex;
  using VertexDegree = common::VertexDegree;

 public:
  MstAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}
  ~MstAppOp(){
      //    delete[] min_out_edge_id_;
  };

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub, scheduler::GraphState* state) override {
    apis::PlanarAppBaseOp<uint32_t>::AppInit(runner, meta, buffer, graphs, hub,
                                             state);

    min_out_edge_id_.resize(meta->num_vertices, MST_INVALID_ID);
    //    min_out_edge_id_ = new uint32_t[meta->num_vertices];
    //    for (uint32_t i = 0; i < meta->num_vertices; i++) {
    //      min_out_edge_id_[i] = MST_INVALID_ID;
    //    }
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
    auto contrac_vertex = [this](VertexID id) { ContractVertex(id); };

    ParallelVertexInitDo(init);
    //    LogVertexState();
    LOG_INFO("init finished!");
    num3 = 100;
    while (GetSubGraphNumEdges() != 0) {
      num_ = 0;
      ParallelVertexDoWithEdges(find_min_edge);
      LOGF_INFO("find min edge finished! edges: {}", num_);
      num3 = num_;

      //      if (num3 < 10) {
      //        LOGF_INFO("id {}, min {}", 76198, min_out_edge_id_[76198]);
      //        LOGF_INFO("id {}, min {}", 76208, min_out_edge_id_[76208]);
      //        LOGF_INFO("id {}, min {}", 76237, min_out_edge_id_[76237]);
      //        LOGF_INFO("id {}, min {}", 76243, min_out_edge_id_[76243]);
      //        LOGF_INFO("id {}, min {}", 76244, min_out_edge_id_[76244]);
      //        LOGF_INFO("id {}, min {}", 76255, min_out_edge_id_[76255]);
      //        LOGF_INFO("id {}, min {}", 76297, min_out_edge_id_[76297]);
      //      }

      num2 = 0;
      ParallelAllVertexDo(graft);
      LOGF_INFO("graft finished! equal num: {}", num2);

      ParallelAllVertexDo(pointer_jump);
      LOG_INFO("pointer jump finished!");

      //      ParallelEdgeMutateDo(contract);
      ParallelVertexDoWithEdges(contrac_vertex);
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
      VertexID min_id = MST_INVALID_ID;
      size_t idx = 0;
      for (VertexDegree i = 0; i < degree; i++) {
        if (IsEdgeDelete(id, i)) continue;
        auto dst = edges[i];
        //        auto flag = util::atomic::WriteMin(&min_out_edge_id_[dst],
        //        id); auto flag = WriteMinSelf(dst, id);
        auto flag = util::atomic::WriteMin(&min_out_edge_id_[dst], id);
        min_id = dst < min_id ? dst : min_id;
        //        {
        //          std::lock_guard<std::mutex> lock(mtx);
        //          num_++;
        //        }
        //        if (num3 < 10) {
        //          LOGF_INFO("src {} -> dst {}, min {} success: {}", id, dst,
        //                    min_out_edge_id_[dst], flag);
        //        }
        idx++;
      }
      if (idx) {
        min_out_edge_id_[id] = min_id;
      }
    }
    //    if (num3 < 10 && id == 76297) {
    //      LOGF_INFO(" Id {} min {}", id, min_out_edge_id_[id]);
    //    }
  }

  void Graft(VertexID src_id) {
    auto dst_id = min_out_edge_id_[src_id];
    //    if (num3 < 10 && src_id == 7619
    //      LOGF_INFO("id {}, min {}", src_id, dst_id);
    //    }
    //    if (num3 < 10 && src_id == 76208) {
    //      LOGF_INFO("id {}, min {}", src_id, dst_id);
    //    }
    //    if (num3 < 10 && src_id == 76237) {
    //      LOGF_INFO("id {}, min {}", src_id, dst_id);
    //    }
    //    if (num3 < 10 && src_id == 76243) {
    //      LOGF_INFO("id {}, min {}", src_id, dst_id);
    //    }

    if (dst_id != MST_INVALID_ID) {
      auto src_parent_id = Read(src_id);
      auto dst_parent_id = Read(dst_id);
      //      if (num3 < 10) {
      //        LOGF_INFO("graft: {}({}) -> {}({})", src_id, src_parent_id,
      //        dst_id,
      //                  dst_parent_id);
      //      }
      if (src_parent_id < dst_parent_id) {
        WriteMin(dst_parent_id, src_parent_id);
      } else if (src_parent_id > dst_parent_id) {
        WriteMin(src_parent_id, dst_parent_id);
      } else {
        //        std::lock_guard<std::mutex> lock(mtx);
        //        num2++;
      }
      min_out_edge_id_[src_id] = MST_INVALID_ID;
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
      //      if (num3 < 10) {
      //        LOGF_INFO("Del edge {} -> {}", src_id, dst_id);
      //      }
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

  void UpdateMinEdge(VertexID id) { min_out_edge_id_[id] = GetNeiMinId(id); }

  void Log() {
    for (VertexID id = 0; id < meta_->num_vertices; id++) {
      LOGF_INFO("Vertex {} min nei is {}", id, min_out_edge_id_[id]);
    }
  }

  bool WriteMinSelf(VertexID id, uint32_t vdata) {
    std::lock_guard<std::mutex> lock(mtx);
    if (min_out_edge_id_[id] <= vdata) return false;
    min_out_edge_id_[id] = vdata;
    return true;
  }

 private:
  std::vector<uint32_t> min_out_edge_id_;
  //  uint32_t* min_out_edge_id_ = nullptr;
  bool mst_active_ = false;
  std::mutex mtx;
  size_t num_ = 0;
  size_t num3 = 0;
  size_t num2 = 0;
  // configs
  bool fast_ = false;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_MST_APP_OP_H
