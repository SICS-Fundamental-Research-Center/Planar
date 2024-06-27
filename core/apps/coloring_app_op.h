#ifndef GRAPH_SYSTEMS_CORE_APPS_COLORING_APP_OP_H
#define GRAPH_SYSTEMS_CORE_APPS_COLORING_APP_OP_H

#include <cstdlib>
#include <random>

#include "apis/planar_app_base_op.h"
#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

class ColoringAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

 public:
  ColoringAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}
  ~ColoringAppOp() override = default;

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub) override {
    apis::PlanarAppBaseOp<uint32_t>::AppInit(runner, meta, buffer, graphs, hub);
    srand(0);
    max_round_ = common::Configurations::Get()->rand_max;
    LOGF_INFO("random: {}", max_round_);
  }

  void PEval() final {
    LOG_INFO("PEval finished!");
    auto init = [this](VertexID id) { Init(id); };
    auto color_vertex = [this](VertexID id) { ColorVertex(id); };

    ParallelVertexInitDo(init);

    app_active_ = 1;
    while (app_active_) {
      app_active_ = 0;
      ParallelVertexDoWithEdges(color_vertex);
      LOGF_INFO("coloring finished, active: {}", app_active_);
    }
  }

  void IncEval() final {
    LOG_INFO("IncEval finished!");
    auto color_vertex = [this](VertexID id) { ColorVertex(id); };

    app_active_ = 1;
    while (app_active_ != 0) {
      app_active_ = 0;
      ParallelVertexDoWithEdges(color_vertex);
      LOGF_INFO("coloring finished, active: {}", app_active_);
    }
  }

  void Assemble() final {}

 private:
  void Init(VertexID id) { Write(id, 0); }

  void ColorVertex(VertexID id) {
    auto degree = GetOutDegree(id);
    if (degree != 0) {
      auto edges = GetOutEdges(id);
      for (VertexDegree i = 0; i < degree; i++) {
        auto dst_id = edges[i];
        if (id < dst_id) {
          auto src_color = Read(id);
          auto dst_color = Read(dst_id);
          if (src_color == dst_color) {
            WriteOneBuffer(id, src_color + GetRandomNumber());
            util::atomic::WriteAdd(&app_active_, 1);
          }
        }
      }
    }
  }

  void ColorVertex2(VertexID id) {
    auto degree = GetOutDegree(id);
    if (degree != 0) {
      auto edges = GetOutEdges(id);
      uint32_t new_color = Read(id);
      for (VertexDegree i = 0; i < degree; i++) {
        auto dst_id = edges[i];
        auto dst_color = Read(dst_id);
        if (id < dst_id && Read(id) == dst_color) {

        }
        if (id < dst_id) {
          auto src_color = Read(id);
          auto dst_color = Read(dst_id);
          if (src_color == dst_color) {
            WriteOneBuffer(id, src_color + 1);
            util::atomic::WriteAdd(&app_active_, 1);
          }
        }
      }
    }
  }

  inline int GetRandomNumber() const { return rand() % max_round_ + 1; }

 private:
  int app_active_ = 0;
  //  common::Bitmap bitmap_;
  int round_ = 0;
  // configs
  uint32_t max_round_ = 10;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_COLORING_APP_OP_H
