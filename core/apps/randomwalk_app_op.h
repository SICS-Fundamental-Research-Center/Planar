#ifndef GRAPH_SYSTEMS_CORE_APPS_RANDOMWALK_APP_OP_H
#define GRAPH_SYSTEMS_CORE_APPS_RANDOMWALK_APP_OP_H

#include <cstdlib>

#include "apis/planar_app_base_op.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

class RandomWalkAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexID = common::VertexID;

 public:
  RandomWalkAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub, scheduler::GraphState* state) override {
    apis::PlanarAppBaseOp<uint32_t>::AppInit(runner, meta, buffer, graphs, hub,
                                             state);
    walk_length_ = core::common::Configurations::Get()->walk;
    num_vertices_ = meta->num_vertices;
    uint64_t size = (uint64_t)(num_vertices_) * (uint64_t)(walk_length_);
    matrix_ = new uint32_t[size];
    LOGF_INFO("matrix size : {}", size);
  }

  ~RandomWalkAppOp() override { delete[] matrix_; };

  void PEval() final {
    auto sample = [this](VertexID id) { Init(id); };
    ParallelVertexDoWithEdges(sample);
    SetActive();
  }

  void IncEval() final {
    auto walk = [this](VertexID id) { this->Walk(id); };
    ParallelVertexDo(walk);
    LOG_INFO("Walk finished");
    UnsetActive();
  }

  void Assemble() final {}

 private:
  void Init(VertexID id) {}

  void Sample(VertexID id) {
    auto degree = GetOutDegree(id);
    auto edges = GetOutEdges(id);
    for (uint32_t i = 0; i < walk_length_; i++) {
      if (degree == 0) {
        GetRoad(id)[i] = id;
      } else {
        GetRoad(id)[i] = edges[GetRandom(degree)];
      }
    }
  }

  void Walk(VertexID id) {
    auto tmp = id;
    for (uint32_t i = 0; i < walk_length_; i++) {
      tmp = GetRoad(tmp)[i];
    }
  }

  uint32_t GetRandom(uint32_t max) {
    uint32_t res = rand() % max;
    return res;
  }

  uint32_t* GetRoad(VertexID id) {
    uint64_t index = (uint64_t)(id) * (uint64_t)(walk_length_);
    return matrix_ + index;
  }

 private:
  // configs
  uint32_t walk_length_ = 5;
  uint32_t* matrix_ = nullptr;
  uint32_t num_vertices_ = 0;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_RANDOMWALK_APP_OP_H
