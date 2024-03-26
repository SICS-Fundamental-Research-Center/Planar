#ifndef GRAPH_SYSTEMS_CORE_APPS_K_HOP_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_K_HOP_APP_H_

#include "apis/planar_app_base.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"
#include "util/atomic.h"

#define MST_INVALID_VID 0xffffffff

namespace sics::graph::core::apps {

using CSRGraph = data_structures::graph::MutableCSRGraphUInt32;

class KHopApp : public apis::PlanarAppBase<CSRGraph> {
  using VertexID = common::VertexID;
  using EdgeIndex = common::EdgeIndex;

 public:
  using VertexData = CSRGraph::VertexData;
  using EdgeData = CSRGraph::EdgeData;
  KHopApp() = default;
  explicit KHopApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph)
      : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {}
  ~KHopApp() override = default;

  void AppInit(common::TaskRunner* runner,
               update_stores::BspUpdateStore<VertexData, EdgeData>*
                   update_store) override {
    apis::PlanarAppBase<CSRGraph>::AppInit(runner, update_store);
  }

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_K_HOP_APP_H_
