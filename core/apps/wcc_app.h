#ifndef GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_

#include <unordered_map>

#include "apis/planar_app_base.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

using CSRGraph = data_structures::graph::MutableCSRGraph<common::VertexID, int>;

class WCCApp : public apis::PlanarAppBase<CSRGraph> {
  using VertexData = typename CSRGraph::VertexData;
  using EdgeData = typename CSRGraph::EdgeData;

  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;

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
  void init(VertexID id);

  void graft(VertexID src_id, VertexID dst_id);

  void point_jump(VertexID src_id);

  void contract(VertexID src_id, VertexID dst_id, EdgeIndex idx);

  // TODO: judge if the vertex is active, this can be done in parallelDo
  void message_passing(VertexID id);

  void point_jump_inc(VertexID id);

 private:
  std::unordered_map<VertexID, VertexID> id_to_p;
  std::mutex mtx;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_
