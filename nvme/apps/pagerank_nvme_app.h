#ifndef GRAPH_SYSTEMS_NVME_APPS_PAGERANK_NVME_APP_H_
#define GRAPH_SYSTEMS_NVME_APPS_PAGERANK_NVME_APP_H_

#include "core/apis/planar_app_base.h"
#include "core/common/types.h"
#include "nvme/apis/block_api.h"
#include "nvme/data_structures/graph/pram_block.h"

namespace sics::graph::nvme::apps {

struct PrData {
  uint32_t pr;
  uint32_t sum;
  PrData(uint32_t pr) : pr(pr), sum(0) {}
};

class PageRankPullApp
    : public apis::BlockModel<core::common::FloatVertexDataType> {
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
        iter(core::common::Configurations::Get()->pr_iter) {}
  ~PageRankPullApp() override = default;

  void Init(VertexID id) {
    float init_value = 1.0 / this->GetOutDegree(id);
    this->Write(id, init_value);
  }

  void PullByEdge(VertexID src_id, VertexID dst_id) {
    auto dst_value = Read(dst_id);
    this->WriteAddDirect(src_id, dst_value);
  }

  // TODO: float atomic should be considered carefully
  void PushByEdge(VertexID src_id, VertexID dst_id) {
    if (step == 0) {
      auto src_value = Read(src_id);
      auto dst_value = Read(dst_id);
      this->Write(dst_id, kLambda * dst_value + kDampingFactor * src_value);
    } else {
      auto src_value = Read(src_id);
      this->WriteAdd(dst_id, kDampingFactor * src_value);
    }
  }

  void DivideDegree(VertexID id) {
    if (step == iter - 1) {
      auto value = Read(id);
      auto pr_new = value * kDampingFactor + kLambda;
      this->Write(id, pr_new);
    } else {
      auto value = Read(id);
      auto degree = this->GetOutDegree(id);
      auto pr_new = (value * kDampingFactor + kLambda) / degree;
      this->Write(id, pr_new);
    }
  }

  void Compute() override {
    LOG_INFO("PageRank Compute() begin!");
    FuncVertex init = [&](VertexID id) { Init(id); };
    FuncEdge pull = [&](VertexID src_id, VertexID dst_id) {
      PullByEdge(src_id, dst_id);
    };
    FuncVertex divide = [&](VertexID id) { DivideDegree(id); };

    MapVertex(&init);
    update_store_.Sync(true);
    update_store_.ResetWriteBuffer();
    //    update_store_.LogVertexData();
    for (; step < iter; step++) {
      MapEdge(&pull);
      //      update_store_.LogVertexData();
      update_store_.Sync(true);

      //      update_store_.LogVertexData();
      MapVertex(&divide);
      update_store_.Sync(true);
      update_store_.ResetWriteBuffer();
      //      update_store_.LogVertexData();
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
};

}  // namespace sics::graph::nvme::apps

#endif  // GRAPH_SYSTEMS_NVME_APPS_PAGERANK_NVME_APP_H_
