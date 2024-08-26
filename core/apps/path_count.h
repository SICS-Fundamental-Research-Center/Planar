#ifndef GRAPH_SYSTEMS_CORE_APPS_PATH_COUNT_H
#define GRAPH_SYSTEMS_CORE_APPS_PATH_COUNT_H

#include <unordered_map>

#include "apis/planar_app_base_op.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

class PathCountAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

 public:
  PathCountAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}
  ~PathCountAppOp() override = default;

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub, scheduler::GraphState* state) override {
    apis::PlanarAppBaseOp<uint32_t>::AppInit(runner, meta, buffer, graphs, hub,
                                             state);
    length = core::common::Configurations::Get()->path_length;
  }

  void PEval() final {
    auto init = [this](VertexID id) { Init(id); };
    auto hop_2_count = [this](VertexID id) { Hop2Count(id); };
    auto hop_3_count = [this](VertexID id) { Hop3Count(id); };

    LOG_INFO("PEval begins");
    ParallelVertexInitDo(init);
    //    LogVertexState();
    ParallelVertexDoWithEdges(hop_2_count);
    int path = 3;
    LOGF_INFO("{} path finish!", path);
    path++;
    for (uint32_t i = 3; i < length; i++) {
      ParallelVertexDoWithEdges(hop_3_count);
      LOGF_INFO("{} path finish!", path);
      path++;
    }
    UnsetActive();
    LOG_INFO("PEval finishes");
  }

  void IncEval() final {
    LOG_INFO("IncEval begins");

    LOG_INFO("IncEval finishes");
  }

  void Assemble() final {}

 private:
  void Init(VertexID id) { Write(id, 0); }

  void Hop2Count(VertexID src) {
    auto degree = GetOutDegree(src);
    if (degree == 0) return;
    auto edges = GetOutEdges(src);
    uint32_t sum = 0;
    for (VertexDegree i = 0; i < degree; i++) {
      auto nei = edges[i];
      auto degree1 = GetOutDegree(nei);
      sum += degree1;
    }
    Write(src, sum);
  }

  void Hop3Count(VertexID src) {
    auto degree = GetOutDegree(src);
    if (degree == 0) return;
    auto edges = GetOutEdges(src);
    uint32_t sum = 0;
    for (VertexDegree i =0; i < degree; i++) {
      auto nei = edges[i];
      auto degree2 = Read(nei);
      sum += degree2;
    }
    Write(src, sum);
  }

 private:
  uint32_t length = 4;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_PATH_COUNT_H
