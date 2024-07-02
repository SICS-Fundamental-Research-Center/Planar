#ifndef GRAPH_SYSTEMS_CORE_APPS_PAGERANK_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_PAGERANK_APP_H_

#include "apis/planar_app_base_op.h"
#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph/mutable_block_csr_graph.h"

namespace sics::graph::core::apps {

using CSRGraph = data_structures::graph::MutableBlockCSRGraph;

class PageRankApp : public apis::PlanarAppBaseOp<float> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

  using VertexData = float;

 public:
  PageRankApp()
      : apis::PlanarAppBaseOp<float>(),
        iter(core::common::Configurations::Get()->pr_iter) {}
  ~PageRankApp() override = default;

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub, scheduler::GraphState* state) override {
    apis::PlanarAppBaseOp<float>::AppInit(runner, meta, buffer, graphs, hub,
                                          state);
    iter = core::common::Configurations::Get()->pr_iter;
    vertexNum_ = meta_->num_vertices;
  }

  void PEval() final {
    LOG_INFO("PEval begins!");
    auto count_degree = [this](VertexID id) { CountDegree(id); };
    auto init = [this](VertexID id) { Init(id); };
    auto pull = [this](VertexID id) { Pull(id); };

    ParallelVertexInitDo(init);
    //    LogVertexState();

    ParallelVertexDoWithEdges(pull);

    SetActive();

    LOG_INFO("PEval finished!");
  }
  void IncEval() final {
    LOG_INFO("IncEval begins!");
    auto pull = [this](VertexID id) { Pull(id); };

    ParallelVertexDoWithEdges(pull);

    if (round_ >= int(iter - 1)) {
      UnsetActive();
    } else {
      SetActive();
    }
    LOG_INFO("IncEval finished!");
  }
  void Assemble() final {}

 private:
  void CountDegree(VertexID id) {
    VertexDegree degree = GetOutDegree(id);
    id2degree_[id] += degree;
  }

  void Init(VertexID id) {
    auto degree = GetOutDegree(id);
    if (degree != 0) {
      Write(id, 1.0 / degree);
    } else {
      Write(id, 1.0 / vertexNum_);
    }
  }

  void Pull(VertexID id) {
    auto degree = GetOutDegree(id);
    if (degree != 0) {
      auto edges = GetOutEdges(id);
      float sum = 0;
      for (VertexDegree i = 0; i < degree; i++) {
        sum += Read(edges[i]);
      }
      float pr_new = 0;
      if (round_ == int(iter)) {
        pr_new = kDampingFactor * sum;
      } else {
        pr_new = (kDampingFactor * sum) / degree;
      }
      Write(id, pr_new);
    }
  }

  void MessagePassing(VertexID id) {}

  void Pull_im(VertexID id) {}

  void Init_im(VertexID id) {}

  void LogDegree() {
    for (VertexID id = 0; id < vertexNum_; id++) {
      LOGF_INFO("Degree of vertex {} is {}", id, id2degree_[id]);
    }
  }

 private:
  const float kDampingFactor = 0.85;
  const float kLambda = 0.15;
  const float kEpsilon = 1e-6;
  int step = 0;
  uint32_t iter = 10;

  bool in_memory_ = false;

  VertexID vertexNum_;
  VertexDegree* id2degree_ = nullptr;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_PAGERANK_APP_H_
