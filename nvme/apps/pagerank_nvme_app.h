#ifndef GRAPH_SYSTEMS_NVME_APPS_PAGERANK_NVME_APP_H_
#define GRAPH_SYSTEMS_NVME_APPS_PAGERANK_NVME_APP_H_

#include "core/apis/planar_app_base.h"
#include "core/common/types.h"
#include "nvme/apis/array.h"
#include "nvme/apis/block_api.h"
#include "nvme/data_structures/graph/pram_block.h"

namespace sics::graph::nvme::apps {

struct PrData {
  uint32_t pr;
  uint32_t sum;
  PrData(uint32_t pr) : pr(pr), sum(0) {}
};

class PageRankPullApp : public apis::BlockModel {
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexID = core::common::VertexID;

  using FuncVertex = core::common::FuncVertex;
  using FuncEdge = core::common::FuncEdge;
  using FuncEdgeMutate = core::common::FuncEdgeMutate;

 public:
  PageRankPullApp() = default;
  PageRankPullApp(const std::string& root_path)
      : BlockModel(root_path),
        iter(core::common::Configurations::Get()->pr_iter) {
    pr.InitMemory(meta_.num_vertices);
    sum.InitMemory(meta_.num_vertices);
  }
  ~PageRankPullApp() override = default;

  void Init(VertexID id) {
    auto degree = graph_.GetOutDegree(id);
    if (degree != 0) {
      pr[id] = 1.0 / degree;
    } else {
      pr[id] = 1.0 / graph_.GetVerticesNum();
    }
  }

  void Pull(VertexID id) {
    auto degree = graph_.GetOutDegree(id);
    if (degree != 0) {
      auto edges = graph_.GetOutEdges(id);
      sum[id] = 0;
      for (VertexID i = 0; i < degree; i++) {
        sum[id] = sum[id] + pr[i];
      }
    }
  }

  void Update(VertexID id) {
    auto degree = graph_.GetOutDegree(id);
    if (degree != 0) {
      if (round_ == iter) {
        pr[id] = kDampingFactor * sum[id];
      } else {
        pr[id] = (kDampingFactor * sum[id]) / degree;
      }
    }
  }

  void Compute() override {
    LOG_INFO("PageRank Compute() begin!");
    FuncVertex init = [&](VertexID id) { Init(id); };
    FuncVertex pull = [&](VertexID id) { Pull(id); };
    FuncVertex update = [&](VertexID id) { Update(id); };

    MapVertexInit(init);
    pr.Log();
    for (; step < iter; step++) {
      MapVertexWithEdges(pull);
      sum.Log();
      MapVertex(update);
      pr.Log();
      LOGF_INFO(" ============== PageRank step {} =============", step);
    }

    LOG_INFO("PageRank Compute() end!");
  }

 private:
  const float kDampingFactor = 0.85;
  const float kLambda = 0.15;
  const float kEpsilon = 1e-6;
  int step = 0;
  const int iter = 10;

  apis::Array<float> pr;
  apis::Array<float> sum;
};

}  // namespace sics::graph::nvme::apps

#endif  // GRAPH_SYSTEMS_NVME_APPS_PAGERANK_NVME_APP_H_
