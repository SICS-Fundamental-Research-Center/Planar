#ifndef GRAPH_SYSTEMS_CORE_APPS_SSSP_APP_OP_H
#define GRAPH_SYSTEMS_CORE_APPS_SSSP_APP_OP_H

#include "apis/planar_app_base.h"
#include "apis/planar_app_base_op.h"
#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

// Push info version.
// Vertex push the shorter distance to all neighbors.
class SsspAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

 public:
  SsspAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub) override {
    apis::PlanarAppBaseOp<uint32_t>::AppInit(runner, meta, buffer, graphs, hub);
    source_ = common::Configurations::Get()->source;
    active_.Init(meta->num_vertices);
    active_next_.Init(meta->num_vertices);
  }

  ~SsspAppOp() override = default;

  void PEval() final {
    LOG_INFO("PEval begins!");
    auto init = [this](VertexID id) { Init(id); };
    auto relax = [this](VertexID id) { Relax(id); };

    SyncSubGraphActive();
    ParallelVertexInitDo(init);

    while (GetActiveNum() != 0) {
      LOGF_INFO("relax begins, active: {}", GetActiveNum());
      ParallelVertexDoWithEdges(relax);
      SyncSubGraphActive();
      LOGF_INFO("relax finished, active: {}", GetActiveNum());
    }
  }

  void IncEval() final {
    LOG_INFO("IncEval begins!");
    auto relax = [this](VertexID id) { Relax(id); };

    SyncSubGraphActive();
    while (GetActiveNum() != 0) {
      LOGF_INFO("relax begins, active: {}", GetActiveNum());
      ParallelVertexDoWithEdges(relax);
      SyncSubGraphActive();
      LOGF_INFO("relax finished, active: {}", GetActiveNum());
    }
  }

  void Assemble() final {}

 private:
  void Init(VertexID id) {
    if (id == source_) {
      Write(id, 0);
      InitVertexActive(id);
      LOGF_INFO("source of SSSP: {}", id);
    } else {
      Write(id, SSSP_INFINITY);
    }
  }

  // Push version
  void Relax(VertexID id) {
    auto degree = GetOutDegree(id);
    if (degree != 0) {
      auto edges = GetOutEdges(id);
      auto current_dis = Read(id) + 1;
      for (VertexDegree i = 0; i < degree; i++) {
        auto dst_id = edges[i];
        auto dst_dis = Read(dst_id);
        if (current_dis < dst_dis) {
          WriteMin(dst_id, current_dis);
          SetVertexActive(dst_id);
        }
      }
    }
  }

  void LogActive();

 private:
  // TODO: move this bitmap into API to reduce function call stack cost
  //  common::Bitmap active_;
  //  common::Bitmap active_next_round_;
  VertexID source_ = 0;
  bool flag = false;
  bool in_memory_ = false;
  bool radical_ = false;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_SSSP_APP_OP_H
