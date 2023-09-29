#ifndef GRAPH_SYSTEMS_CORE_APPS_MST_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_MST_APP_H_

#include "apis/planar_app_base.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"
#include "util/atomic.h"

#define MST_INVALID_VID 0xffffffff

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
      data_structures::Serializable* graph)
      : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {}
  ~MstApp() override { delete[] min_out_edge_id_; };

  void AppInit(common::TaskRunner* runner,
               update_stores::BspUpdateStore<VertexData, EdgeData>*
                   update_store) override {
    apis::PlanarAppBase<CSRGraph>::AppInit(runner, update_store);
    min_out_edge_id_ = new VertexData[update_store->GetMessageCount()];
    for (int i = 0; i < update_store->GetMessageCount(); i++) {
      min_out_edge_id_[i] = MST_INVALID_VID;
    }
  }

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
  void Init(VertexID id);

  void FindMinEdge(VertexID id);

  void Graft(VertexID src_id);

  void PrepareMinOutEdgeFirst(VertexID id);

  void MessagePassing(VertexID id);

  void PointJump(VertexID id);

  void Contract(VertexID src_id, VertexID dst_id, EdgeIndex idx);

  void UpdateMinEdge(VertexID id);

  void PointJumpInc(VertexID id);

  void LogMinOutEdgeId();

  void WriteMinAuxiliary(VertexID id, VertexData data);

  VertexID ReadAuxiliary(VertexID id);

 private:
  VertexData* min_out_edge_id_;
  std::unordered_map<VertexID, VertexID> id_to_p_;
  std::mutex mtx_;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_MST_APP_H_
