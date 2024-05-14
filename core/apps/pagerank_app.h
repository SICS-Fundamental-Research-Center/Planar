#ifndef GRAPH_SYSTEMS_CORE_APPS_PAGERANK_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_PAGERANK_APP_H_

#include "apis/planar_app_base.h"
#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

using CSRGraph =
    data_structures::graph::MutableCSRGraph<float,
                                            core::common::DefaultEdgeDataType>;

class PageRankApp : public apis::PlanarAppBase<CSRGraph> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

 public:
  using VertexData = CSRGraph::VertexData;
  using EdgeData = CSRGraph::EdgeData;

  PageRankApp() = default;
  explicit PageRankApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph)
      : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph),
        iter(core::common::Configurations::Get()->pr_iter) {}
  ~PageRankApp() override = default;

  void AppInit(common::TaskRunner* runner,
               update_stores::BspUpdateStore<VertexData, EdgeData>*
                   update_store) override {
    apis::PlanarAppBase<CSRGraph>::AppInit(runner, update_store);
    iter = core::common::Configurations::Get()->pr_iter;
    vertexNum_ = update_store->GetMessageCount();
    id2degree_ = new VertexDegree[vertexNum_];
    in_memory_ = core::common::Configurations::Get()->in_memory;
  }

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
  void CountDegree(VertexID id);

  void Init(VertexID id);

  void Pull(VertexID id);

  void MessagePassing(VertexID id);

  void Pull_im(VertexID id);

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
  VertexDegree* id2degree_;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_PAGERANK_APP_H_
