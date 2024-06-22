#ifndef GRAPH_SYSTEMS_CORE_APPS_MST_APP_OP_H
#define GRAPH_SYSTEMS_CORE_APPS_MST_APP_OP_H

#include "apis/planar_app_base.h"
#include "apis/planar_app_base_op.h"
#include "common/types.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph/mutable_csr_graph.h"
#include "util/atomic.h"

#define MST_INVALID_VID 0xffffffff

namespace sics::graph::core::apps {

class MstAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexID = common::VertexID;
  using EdgeIndex = common::EdgeIndex;
  using VertexDegree = common::VertexDegree;

 public:
  MstAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}
  ~MstAppOp() override = default;

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub) override {
    apis::PlanarAppBaseOp<uint32_t>::AppInit(runner, meta, buffer, graphs, hub);
  }

  void PEval() final {}
  void IncEval() final {}
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

  void FindMinEdge(VertexID id) {
    
  }

  void PrepareMinOutEdgeFirst(VertexID id) {
    min_out_edge_id_[id] = GetNeiMinId(id);
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

  void UpdateMinEdge(VertexID id);

  void PointJumpInc(VertexID id);

  void LogMinOutEdgeId() const;

 private:
  uint32_t * min_out_edge_id_;
  // configs
  bool fast_ = false;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_MST_APP_OP_H
