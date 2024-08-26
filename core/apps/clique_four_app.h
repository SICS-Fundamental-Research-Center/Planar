#ifndef GRAPH_SYSTEMS_CORE_APPS_CLIQUE_FOUR_APP_H
#define GRAPH_SYSTEMS_CORE_APPS_CLIQUE_FOUR_APP_H

#include <unordered_map>

#include "apis/planar_app_base_op.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

class CliqueFourAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

 public:
  CliqueFourAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}
  ~CliqueFourAppOp() override = default;

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub, scheduler::GraphState* state) override {
    apis::PlanarAppBaseOp<uint32_t>::AppInit(runner, meta, buffer, graphs, hub,
                                             state);
    clique3.resize(meta_->num_vertices);
  }

  void PEval() final {
    auto init = [this](VertexID id) { Init(id); };
    auto triangle_count = [this](VertexID id) { TriangleCount(id); };
    auto Clique_count = [this](VertexID id) { CliqueFourCount(id); };

    LOG_INFO("PEval begins");
    ParallelVertexDoWithEdges(init);
    //    LogVertexState();
    ParallelVertexDoWithEdges(triangle_count);
    ParallelVertexDoWithEdges(Clique_count);

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
    for (VertexDegree i = 0; i < degree; i++) {
      auto one = edges[i];
      for (VertexDegree j = 0; j < degree; j++) {
        if (j == i) continue;
        auto two = edges[j];
        if (IsNeighbor(one, two)) {
          clique3[src].push_back(data_structures::Pair{one, two});
        }
      }
    }
  }

  void CliqueFourCount(VertexID src) {
    auto degree = GetOutDegree(src);
    if (degree == 0) return;
    auto edges = GetOutEdges(src);
    if (clique3.at(src).size() != 0) {
      auto size = clique3[src].size();
      if (size == 0) return;
      auto& pairs = clique3[src];
      uint32_t sum = 0;
      for (int i = 0; i < size; i++) {
        auto pair = pairs.at(i);
        for (VertexDegree j = 0; j < degree; j++) {
          auto dst = edges[j];
          if (IsNeighbor(dst, pair)) {
            sum++;
          }
        }
      }
      Write(src, sum);
    }
  }

 private:
  std::vector<std::vector<data_structures::Pair>> clique3;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_CLIQUE_FOUR_APP_H
