#ifndef GRAPH_SYSTEMS_CORE_APPS_PAGERANK_APP_OP_H_
#define GRAPH_SYSTEMS_CORE_APPS_PAGERANK_APP_OP_H_

#include "apis/planar_app_base.h"
#include "apis/planar_app_op.h"
#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

class PageRankOpApp : public apis::PlanarAppOpBase<float> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

 public:
  PageRankOpApp() = default;
  PageRankOpApp(const std::string& root_path)
      : apis::PlanarAppOpBase<float>(root_path) {
    AppInit(root_path);
  }
  ~PageRankOpApp() override = default;

  void AppInit(const std::string& root_path) {
    iter = core::common::Configurations::Get()->pr_iter;
    in_memory_ = core::common::Configurations::Get()->in_memory;
  }

  void PEval() {
    LOG_INFO("PEval start");
    auto init = [this](VertexID id) { Init(id); };
    auto pull = [this](VertexID id) { Pull(id); };

    LogVertexState();
    ParallelVertexDo(init);
    LogVertexState();

    ParallelVertexDoWithEdges(pull);

    LOG_INFO("PEval end");
  };

  void IncEval() {
    LOG_INFO("IncEval start");
    ParallelVertexDoWithEdges([this](VertexID id) { Pull(id); });
    LOG_INFO("IncEval end");
  };

  void Assemble(){};

 private:
  void Init(VertexID id) {
    auto degree = GetOutDegree(id);
    if (degree == 0) {
      Write(id, 1.0 / GetVertexNum());
    } else {
      Write(id, 1.0 / degree);
    }
  }

  void Pull(VertexID id) {
    auto degree = GetOutDegree(id);
    if (degree == 0) return;
    auto edges = GetOutEdges(id);
    float sum = 0;
    for (uint32_t i = 0; i < degree; i++) {
      sum += Read(edges[i]);
    }
    float pr_new = 0;
    if (round_ == int(iter)) {
      pr_new = kDampingFactor * sum;
    } else {
      pr_new = (kDampingFactor * sum) / degree;
    }
    WriteAdd(id, pr_new);
  }

 private:
  const float kDampingFactor = 0.85;
  const float kLambda = 0.15;
  const float kEpsilon = 1e-6;
  int step = 0;
  uint32_t iter = 10;

  bool in_memory_ = false;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_PAGERANK_APP_OP_H_
