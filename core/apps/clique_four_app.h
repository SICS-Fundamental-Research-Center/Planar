#ifndef GRAPH_SYSTEMS_CORE_APPS_CLIQUE_FOUR_APP_H
#define GRAPH_SYSTEMS_CORE_APPS_CLIQUE_FOUR_APP_H

#include <unordered_map>

#include "apis/planar_app_base_op.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

class StarAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

 public:
  StarAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}
  ~StarAppOp() override = default;

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub, scheduler::GraphState* state) override {
    apis::PlanarAppBaseOp<uint32_t>::AppInit(runner, meta, buffer, graphs, hub,
                                             state);
    star_num = core::common::Configurations::Get()->path_length;
    label = new uint32_t[meta->num_vertices];
    std::fill(label, label + 25000000, 0);
    std::fill(label + 25000000, label + 40000000, 1);
    std::fill(label + 40000000, label + 45000000, 2);
    std::fill(label + 45000000, label + 50000000, 3);
    std::fill(label + 50000000, label + 50000100, 4);
    LOG_INFO("label mark finish");
  }

  void PEval() final {
    auto init = [this](VertexID id) { Init(id); };
    auto star_three = [this](VertexID id) { StarThree(id); };

    LOG_INFO("PEval begins");
    ParallelVertexDoWithEdges(init);
    //    LogVertexState();
    ParallelVertexDoWithEdges(star_three);
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

  void StarThree(VertexID src) {
    if (label[src] != 0) return;
    auto degree = GetOutDegree(src);
    if (degree < star_num) return;
    auto edges = GetOutEdges(src);
    uint32_t sum = 0;
    for (VertexDegree i = 0; i < degree; i++) {
      auto dst = edges[i];
      if (label[dst] == 0) {
        sum++;
        if (sum == star_num) {
          Write(src, 1);
          break;
        }
      }
    }
  }

 private:
  uint32_t star_num = 3;
  uint32_t* label = nullptr;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_CLIQUE_FOUR_APP_H
