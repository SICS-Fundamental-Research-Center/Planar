#ifndef GRAPH_SYSTEMS_CORE_APPS_SSSP_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_SSSP_APP_H_

#include "apis/planar_app_base.h"
#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"

#define SSSP_INFINITY 0xffffffff

namespace sics::graph::core::apps {

using CSRGraph = data_structures::graph::MutableCSRGraphUInt32;

// Push info version.
// Vertex push the shorter distance to all neighbors.
class SsspApp : public apis::PlanarAppBase<CSRGraph> {
  using VertexID = common::VertexID;

 public:
  using VertexData = CSRGraph::VertexData;
  using EdgeData = CSRGraph::EdgeData;
  SsspApp() = default;
  explicit SsspApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph)
      : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {
    active_.Init(update_store_->GetMessageCount());
  }

  ~SsspApp() override = default;

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
  void Init(VertexID id);

  void Relax(VertexID id);

  void MessagePassing(VertexID id);

 private:
  // TODO: move this bitmap into API to reduce function call stack cost
  common::Bitmap active_;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_SSSP_APP_H_
