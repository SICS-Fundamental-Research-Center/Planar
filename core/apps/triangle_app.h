#ifndef GRAPH_SYSTEMS_CORE_APPS_TRIANGLE_APP_H
#define GRAPH_SYSTEMS_CORE_APPS_TRIANGLE_APP_H

#include <unordered_map>

#include "apis/planar_app_base_op.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

class TriangleAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

  struct Pair {
    core::common::VertexID one;
    core::common::VertexID two;
  };

 public:
  TriangleAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}
  ~TriangleAppOp() override = default;

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
    auto triangle_count = [this](VertexID id) { TriangleCount(id); };

    LOG_INFO("PEval begins");
    ParallelVertexDoWithEdges(init);
    //    LogVertexState();
    ParallelVertexDoWithEdges(triangle_count);

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

  void TriangleCount(VertexID src) {
    auto degree = GetOutDegree(src);
    if (degree < 2) return;
    auto edges = GetOutEdges(src);
    uint32_t sum = 0;
    for (VertexDegree i = 0; i < degree; i++) {
      auto one = edges[i];
      for (VertexDegree j = 0; j < degree; j++) {
        if (j == i) continue;
        auto two = edges[j];
        if (IsNeighbor(one, two)) {
          sum++;
        }
      }
    }
    Write(src, sum);
  }

 private:
  std::unordered_map<VertexID, std::vector<VertexID>> clique3;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_TRIANGLE_APP_H
