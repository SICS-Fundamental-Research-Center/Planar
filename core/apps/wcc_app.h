#ifndef GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_

#include "apis/planar_app_base.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

struct WccVertexData {
  common::VertexID p;
  WccVertexData() = default;
  WccVertexData(common::VertexID p) : p(p) {}
};

using CSRGraph = data_structures::graph::MutableCSRGraph<WccVertexData, int>;

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
  void init(VertexID id) { graph_->Write(id, WccVertexData(id)); }

  void graft(VertexID src_id, VertexID dst_id) {
    VertexID src_parent_id = graph_->Read(src_id)->p;
    VertexID dst_parent_id = graph_->Read(dst_id)->p;
    if (src_parent_id != dst_parent_id) {
      if (src_parent_id < dst_parent_id) {
        graph_->Write(dst_parent_id,
                      WccVertexData(graph_->Read(src_parent_id)->p));
      } else {
        graph_->Write(src_parent_id,
                      WccVertexData(graph_->Read(dst_parent_id)->p));
      }
    }
    // TODO: add UpdateStore info logic
  }

  void point_jump(VertexID src_id) {
    VertexData parent;
    parent.p = graph_->Read(src_id)->p;
    if (parent.p != graph_->Read(parent.p)->p) {
      while (parent.p != graph_->Read(parent.p)->p) {
        parent.p = graph_->Read(parent.p)->p;
      }
      graph_->Write(src_id, parent);
    }
    // TODO: update vertex global data in update_store
  }

  void contract(VertexID src_id, VertexID dst_id, EdgeIndex idx) {
    if (graph_->Read(src_id)->p == graph_->Read(dst_id)->p) {
      graph_->DeleteEdge(idx);
    }
  }

  // TODO: judge if teh vertex is active, this can be done in parallelDo
  void message_passing(VertexID id) {
    if (update_store_->Read(id)->p < graph_->Read(id)->p) {
      if () {

      } else {

      }
    }
  }

  void point_jump_inc(VertexID id) {}
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_
