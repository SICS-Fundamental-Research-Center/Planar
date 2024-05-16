#ifndef GRAPH_SYSTEMS_CORE_APPS_TEST_H_
#define GRAPH_SYSTEMS_CORE_APPS_TEST_H_

#include "apis/planar_app_base.h"
#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

using CSRGraph =
    data_structures::graph::MutableCSRGraph<float,
                                            core::common::DefaultEdgeDataType>;

class TestApp : public apis::PlanarAppBase<CSRGraph> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

 public:
  using VertexData = CSRGraph::VertexData;
  using EdgeData = CSRGraph::EdgeData;

  TestApp() = default;
  explicit TestApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph)
      : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {}
  ~TestApp() override = default;

  void AppInit(common::TaskRunner* runner,
               update_stores::BspUpdateStore<VertexData, EdgeData>*
                   update_store) override {
    apis::PlanarAppBase<CSRGraph>::AppInit(runner, update_store);
  }

  void PEval() {
    LOG_INFO("TestApp PEval begins!\n");

    for (VertexID id = 0; id < 100; id++) {
      //      std::string info = "VertexID: " + std::to_string(id) + " ";
      auto degree = graph_->GetOutDegreeByID(id);
      //      info += "Degree: " + std::to_string(degree) + " Edges:";
      auto edges = graph_->GetOutEdgesByID(id);
      for (int i = 0; i < degree; i++) {
        if (id == edges[i]) {
          LOGF_INFO("{} has self-loop!", id);
        }
      }
      update_store_->UnsetActive();
      //      LOG_INFO(info.c_str());
    }
    LOG_INFO("TestApp PEval finished!\n");
  }
  void IncEval() {}
  void Assemble() {}

 private:
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_TEST_H_
