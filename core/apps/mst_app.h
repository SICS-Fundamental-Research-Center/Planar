#ifndef GRAPH_SYSTEMS_CORE_APPS_MST_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_MST_APP_H_

#include "apis/planar_app_base.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

using CSRGraph = data_structures::graph::MutableCSRGraphUInt32;

class MstApp : public apis::PlanarAppBase<CSRGraph> {
  using VertexID = common::VertexID;
  using EdgeIndex = common::EdgeIndex;

 public:
  using VertexData = CSRGraph::VertexData;
  using EdgeData = CSRGraph::EdgeData;
  MstApp() = default;
  explicit MstApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph);
  ~MstApp() override = default;

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
  void Init(VertexID id);

  void Graft(VertexID src_id);

  void MessagePassing(VertexID id);

  void PointJump(VertexID id);

  void Contract(VertexID src_id, VertexID dst_id, EdgeIndex idx);
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_MST_APP_H_
