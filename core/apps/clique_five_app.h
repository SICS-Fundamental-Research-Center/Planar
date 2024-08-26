#ifndef GRAPH_SYSTEMS_CORE_APPS_CLIQUE_FIVE_APP_H
#define GRAPH_SYSTEMS_CORE_APPS_CLIQUE_FIVE_APP_H

#include <unordered_map>

#include "apis/planar_app_base_op.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

class CliqueFiveAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

  struct Pair {
    core::common::VertexID one;
    core::common::VertexID two;
  };
  struct Tri {
    core::common::VertexID one;
    core::common::VertexID two;
    core::common::VertexID three;
  };

 public:
  CliqueFiveAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}
  ~CliqueFiveAppOp() override = default;

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
    auto clique_four_count = [this](VertexID id) { CliqueFourCount(id); };
    auto clique_five_count = [this](VertexID id) { CliqueFiveCount(id);};

    LOG_INFO("PEval begins");
    ParallelVertexDoWithEdges(init);
    //    LogVertexState();
    ParallelVertexDoWithEdges(triangle_count);
    ParallelVertexDoWithEdges(clique_four_count);
    ParallelVertexDoWithEdges(clique_five_count);

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
          if (clique3.find(src) == clique3.end()) {
            clique3[src] = {};
          }
          clique3[src].push_back(Pair{one, two});
        }
      }
    }
  }

  void CliqueFourCount(VertexID src) {
    auto degree = GetOutDegree(src);
    if (degree == 0) return;
    auto edges = GetOutEdges(src);
    if (clique3.find(src) != clique3.end()) {
      auto size = clique3[src].size();
      if (size == 0) return;
      auto& pairs = clique3[src];
      for (int i = 0; i < size; i++) {
        auto pair = pairs.at(i);
        for (VertexDegree j = 0; j < degree; j++) {
          auto dst = edges[j];
          if (IsNeighbor(dst, pair.one) && IsNeighbor(dst, pair.two)) {
            if (clique4.find(src) == clique4.end()) {
              clique4[src] = {};
            }
            clique4[src].push_back(Tri{dst, pair.one, pair.two});
          }
        }
      }
    }
  }

  void CliqueFiveCount(VertexID src) {
    auto degree = GetOutDegree(src);
    if (degree == 0) return;
    auto edges = GetOutEdges(src);
    if (clique4.find(src) != clique4.end()) {
      auto size = clique4[src].size();
      if (size == 0) return;
      auto& tris = clique4[src];
      uint32_t sum = 0;
      for (int i = 0; i < size; i++) {
        auto tri = tris.at(i);
        for (VertexDegree j = 0; j < degree; j++) {
          auto dst = edges[j];
          if (IsNeighbor(dst, tri.one) && IsNeighbor(dst, tri.two) &&
              IsNeighbor(dst, tri.three)) {
            sum++;
          }
        }
      }
      Write(src, sum);
    }
  }

 private:
  std::unordered_map<VertexID, std::vector<Pair>> clique3;
  std::unordered_map<VertexID, std::vector<Tri>> clique4;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_CLIQUE_FIVE_APP_H
