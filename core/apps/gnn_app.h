#ifndef GRAPH_SYSTEMS_CORE_APPS_GNN_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_GNN_APP_H_

#include "apis/planar_app_base.h"
#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

using CSRGraph =
    data_structures::graph::MutableCSRGraph<float,
                                            core::common::DefaultEdgeDataType>;

class GNNApp : public apis::PlanarAppBase<CSRGraph> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

 public:
  using VertexData = CSRGraph::VertexData;
  using EdgeData = CSRGraph::EdgeData;

  GNNApp() = default;
  explicit GNNApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph)
      : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph),
        iter(core::common::Configurations::Get()->pr_iter) {}
  ~GNNApp() override = default;

  void AppInit(common::TaskRunner* runner,
               update_stores::BspUpdateStore<VertexData, EdgeData>*
                   update_store) override {
    apis::PlanarAppBase<CSRGraph>::AppInit(runner, update_store);
    iter = core::common::Configurations::Get()->pr_iter;
    vertexNum_ = update_store->GetMessageCount();
    id2degree_ = new VertexDegree[vertexNum_];
    w = rand_float();
    b = rand_float();
  }

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
  void Init(VertexID id);

  void Forward(VertexID id);

  void MessagePassing(VertexID id);

  inline float spmv(float w, float v);

  float sigmod(float x);

  float rand_float() {
    return static_cast<float>(rand()) / static_cast<float>(RAND_MAX) * 2 - 1;
  }

  int rand_num_egdes(uint32_t degree) {
    return rand() % degree;
  }

  bool if_take() {
    return static_cast<float>(rand()) / static_cast<float>(RAND_MAX) < 0.6;
  }

  void LogDegree() {
    for (VertexID id = 0; id < vertexNum_; id++) {
      LOGF_INFO("Degree of vertex {} is {}", id, id2degree_[id]);
    }
  }

 private:
  uint32_t iter = 10;

  float w;
  float b;

  VertexID vertexNum_;
  VertexDegree* id2degree_;
  int step = 0;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_GNN_APP_H_
