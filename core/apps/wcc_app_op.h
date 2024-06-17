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

 public:
  WCCAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}
  ~WCCAppOp() override = default;

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub) override {
    apis::PlanarAppBaseOp<uint32_t>::AppInit(runner, meta, buffer, graphs, hub);
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

    ParallelVertexInitDo(init);
    LogVertexState();

    LOG_INFO("PEval begins");
    auto size = GetSubGraphNumEdges();
    auto last_size = size;
    while (true) {
      ParallelEdgeMutateDo(graft);
      LOG_INFO("Graft finishes");
      LogVertexState();
      ParallelVertexDo(pointer_jump);
      LOG_INFO("Pointer jump finishes");
      LogVertexState();
      ParallelEdgeMutateDo(contract);
      LOG_INFO("Contract finishes!");
      size = GetSubGraphNumEdges();
      LogCurrentGraphVertexState();
      LogCurrentGraphDelInfo();
      if (size == last_size) {
        break;
      } else {
        last_size = size;
      }
    }
    LOG_INFO("PEval finishes");
  }

  void IncEval() final {
    auto graft = [this](VertexID src_id, VertexID dst_id) {
      Graft(src_id, dst_id);
    };
    auto pointer_jump = [this](VertexID id) { PointJump(id); };
    auto contract = [this](VertexID src_id, VertexID dst_id, EdgeIndex idx) {
      Contract(src_id, dst_id, idx);
    };
    LOG_INFO("IncEval begins");
    auto size = GetSubGraphNumEdges();
    if (size == 0) {
      ParallelVertexDo(pointer_jump);
    } else {
      auto last_size = size;
      while (true) {
        ParallelEdgeMutateDo(graft);
        LOG_INFO("Graft finishes");
        LogVertexState();
        ParallelVertexDo(pointer_jump);
        LOG_INFO("Pointer jump finishes");
        LogVertexState();
        ParallelEdgeMutateDo(contract);
        LOG_INFO("Contract finishes!");
        size = GetSubGraphNumEdges();
        if (size == last_size) {
          break;
        } else {
          last_size = size;
        }
      }
    }
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
    auto parent_id = Read(src_id);
    if (parent_id != Read(parent_id)) {
      while (parent_id != Read(parent_id)) {
        parent_id = Read(parent_id);
      }
    }
    Write(src_id, parent_id);
  }

  void Contract(VertexID src_id, VertexID dst_id, EdgeIndex idx) {
    if (Read(src_id) == Read(dst_id)) {
      DeleteEdge(src_id, idx);
    }
  }

  void PointJumpIncEval(VertexID id);

 private:
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_WCC_APP_OP_H
