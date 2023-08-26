#ifndef GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_

#include "apis/planar_app_base.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

struct WccVertexData {
  int p;
  WccVertexData() = default;
  WccVertexData(int p) : p(p) {}
};

using CSRGraph = data_structures::graph::MutableCSRGraph<int, int>;

class WCCApp : public apis::PlanarAppBase<CSRGraph> {
  using VertexData = typename CSRGraph::VertexData;
  using EdgeData = typename CSRGraph::EdgeData;
  using EdgeIndex = common::EdgeIndex;

 public:
  explicit WCCApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph);
  ~WCCApp() override = default;

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
  void init(common::VertexID id) {
    // TODO: init for vertex
    graph_->GetOutDegreeByIndex(id);
    auto a = graph_->GetVertxDataByIndex(id);

    LOG_INFO("test for init");
  }

  void graft(common::VertexID src_id, common::VertexID dst_id) {
    //    auto t = update_store_->Read(src_id);
    //    graph_->Write(src_id, *t);
    // TODO: graft for vertex
    LOG_INFO("test for graft");
  }

  void contract(common::VertexID src_id, common::VertexID dst_id,
                EdgeIndex idx) {
    //    auto t = update_store_->Read(src_id);
    //    graph_->Write(src_id, *t);
    // TODO: contract for vertex
    LOG_INFO("test for contract");
  }
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_
