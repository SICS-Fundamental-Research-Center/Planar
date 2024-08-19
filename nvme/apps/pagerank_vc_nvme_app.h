#ifndef GRAPH_SYSTEMS_NVME_APPS_PAGERANK_VC_NVME_APP_H_
#define GRAPH_SYSTEMS_NVME_APPS_PAGERANK_VC_NVME_APP_H_

#include "core/apis/planar_app_base.h"
#include "core/common/types.h"
#include "nvme/apis/block_api.h"
#include "nvme/data_structures/graph/pram_block.h"

namespace sics::graph::nvme::apps {

class PageRankVCApp
    : public apis::BlockModel<core::common::FloatVertexDataType> {
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexID = core::common::VertexID;
  using VertexDegree = core::common::VertexDegree;

  using FuncVertex = core::common::FuncVertex;
  using FuncEdge = core::common::FuncEdge;
  using FuncEdgeMutate = core::common::FuncEdgeMutate;

 public:
  PageRankVCApp() = default;
  PageRankVCApp(const std::string& root_path)
      : BlockModel(root_path),
        iter(core::common::Configurations::Get()->pr_iter) {}
  ~PageRankVCApp() override = default;

  void Init(VertexID id) {
    auto degree = this->GetOutDegree(id);
    if (degree != 0) {
      this->Write(id, 1.0 / degree);
    } else {
      this->Write(id, 1.0 / this->GetNumVertices());
    }
  }

  void PullByVertex(VertexID src_id) {
    auto degree = this->GetOutDegree(src_id);
    if (degree != 0) {
      float sum = 0;
      auto edges = this->GetEdges(src_id);
      for (VertexDegree i = 0; i < degree; i++) {
        sum += Read(edges[i]);
      }
      float pr_new = 0;
      if (step == iter - 1) {
        pr_new = sum * kDampingFactor + kLambda;
      } else {
        pr_new = (sum * kDampingFactor) / degree + kLambda;
      }
      this->Write(src_id, pr_new);
    }
  }

  void Compute() override {
    LOG_INFO("PageRank VC Compute() begin!");
    FuncVertex init = [&](VertexID id) { Init(id); };
    FuncVertex pull = [&](VertexID src_id) { PullByVertex(src_id); };

    MapVertex(&init);
    LOG_INFO(" ============ PageRank init finish! =========== ");
    //    update_store_.LogVertexData();
    for (; step < iter; step++) {
      MapVertex(&pull);
      //      update_store_.LogVertexData();
      LOGF_INFO(" ============== PageRank step {} =============", step);
    }

    LOG_INFO("PageRank VC Compute() end!");
  }

 private:
  const float kDampingFactor = 0.85;
  const float kLambda = 0.15;
  const float kEpsilon = 1e-6;
  uint32_t step = 0;
  const uint32_t iter = 10;
};

}  // namespace sics::graph::nvme::apps

#endif  // GRAPH_SYSTEMS_NVME_APPS_PAGERANK_VC_NVME_APP_H_
