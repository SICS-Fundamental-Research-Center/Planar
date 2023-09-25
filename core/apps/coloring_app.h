#ifndef GRAPH_SYSTEMS_CORE_APPS_COLORING_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_COLORING_APP_H_

#include "apis/planar_app_base.h"
#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

using CSRGraph = data_structures::graph::MutableCSRGraphUInt32;

class ColoringApp : public apis::PlanarAppBase<CSRGraph> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;

 public:
  using VertexData = CSRGraph::VertexData;
  using EdgeData = CSRGraph::EdgeData;
  ColoringApp() = default;
  explicit ColoringApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph)
      : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {}
  ~ColoringApp() override =default;

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
  void Init(VertexID id);

  void FindConflictsVertex(VertexID src_id, VertexID dst_id);

  void FindConflictsColor(VertexID src_id, VertexID dst_id);

  void Color(VertexID id);

  void MessagePassing(VertexID id);

  void ColorNew(VertexID src_id, VertexID dst_id);

 private:
  bool active_ = false;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_COLORING_APP_H_
