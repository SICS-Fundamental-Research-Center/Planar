#ifndef GRAPH_SYSTEMS_NVME_APPS_PAGERANK_NVME_PULL_APP_H_
#define GRAPH_SYSTEMS_NVME_APPS_PAGERANK_NVME_PULL_APP_H_

#include "core/apis/planar_app_base.h"
#include "nvme/apis/block_api.h"
#include "nvme/data_structures/graph/pram_block.h"

namespace sics::graph::nvme::apps {

class PageRankPullApp : public apis::BlockModel<core::common::FloatVertexDataType> {
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexID = core::common::VertexID;

 public:
  PageRankPullApp() = default;
  PageRankPullApp(const std::string& root_path) : BlockModel(root_path) {}
  ~PageRankPullApp() override = default;

  void Init(VertexID id) { this->Write(id, 1); }

  void PullByVertex(VertexID id) {
    auto src_value = Read(id);
    float sum =0;
  }

  void PullByEdge() {

  }

  void Compute() override { LOG_INFO("PageRank Compute() begin!"); }

 private:
};

}  // namespace sics::graph::nvme::apps

#endif  // GRAPH_SYSTEMS_NVME_APPS_PAGERANK_NVME_PULL_APP_H_
