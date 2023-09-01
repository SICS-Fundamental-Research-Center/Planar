#ifndef GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_

#include <unordered_map>

#include "apis/planar_app_base.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

using CSRGraph = data_structures::graph::MutableCSRGraphUInt32;

class WCCApp : public apis::PlanarAppBase<CSRGraph> {
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;

 public:
  using VertexData = typename CSRGraph::VertexData;
  using EdgeData = typename CSRGraph::EdgeData;
  WCCApp() = default;
  explicit WCCApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph);
  ~WCCApp() override = default;

  void SetRuntimeGraph(CSRGraph* graph);

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
  void Init(VertexID id);

  void Graft(VertexID src_id, VertexID dst_id);

  void PointJump(VertexID src_id);

  void Contract(VertexID src_id, VertexID dst_id, EdgeIndex idx);

  // TODO: judge if the vertex is active, this can be done in parallelDo
  void MessagePassing(VertexID id);

  void PointJumpIncEval(VertexID id);

 private:
  std::unordered_map<VertexID, VertexID> id_to_p;
  std::mutex mtx;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_
