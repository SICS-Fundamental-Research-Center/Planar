#include "apps/wcc_app.h"

namespace sics::graph::core::apps {

// Note: check whether the app is registered.
WCCApp::WCCApp(
    common::TaskRunner* runner,
    update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
    data_structures::Serializable* graph)
    : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {}

void WCCApp::PEval() {
  ParallelVertexDo(std::bind(&WCCApp::init, this, std::placeholders::_1));
  while (graph_->GetOutEdgeNums() != 0) {
    ParallelEdgeDo(std::bind(&WCCApp::graft, this, std::placeholders::_1,
                             std::placeholders::_2));

    ParallelVertexDo(
        std::bind(&WCCApp::point_jump, this, std::placeholders::_1));

    ParallelEdgeMutateDo(std::bind(&WCCApp::contract, this,
                                   std::placeholders::_1, std::placeholders::_2,
                                   std::placeholders::_3));
  }
  graph_->set_status("PEval");
}

void WCCApp::IncEval() {
  ParallelVertexDo(
      std::bind(&WCCApp::message_passing, this, std::placeholders::_1));

  ParallelVertexDo(
      std::bind(&WCCApp::point_jump_inc, this, std::placeholders::_1));

  graph_->set_status("IncEval");
}

void WCCApp::Assemble() { graph_->set_status("Assemble"); }

void WCCApp::init(VertexID id) {
  graph_->WriteLocalVertexData(id, WccVertexData(id));
}

void WCCApp::graft(VertexID src_id, VertexID dst_id) {
  VertexID src_parent_id = graph_->ReadLocalVertexData(src_id)->p;
  VertexID dst_parent_id = graph_->ReadLocalVertexData(dst_id)->p;
  if (src_parent_id != dst_parent_id) {
    if (src_parent_id < dst_parent_id) {
      graph_->WriteLocalVertexData(
          dst_parent_id,
          WccVertexData(graph_->ReadLocalVertexData(src_parent_id)->p));
    } else {
      graph_->WriteLocalVertexData(
          src_parent_id,
          WccVertexData(graph_->ReadLocalVertexData(dst_parent_id)->p));
    }
  }
  // TODO: add UpdateStore info logic
}

void WCCApp::point_jump(VertexID src_id) {
  VertexData parent;
  parent.p = graph_->ReadLocalVertexData(src_id)->p;
  if (parent.p != graph_->ReadLocalVertexData(parent.p)->p) {
    while (parent.p != graph_->ReadLocalVertexData(parent.p)->p) {
      parent.p = graph_->ReadLocalVertexData(parent.p)->p;
    }
    graph_->WriteLocalVertexData(src_id, parent);
  }
  // TODO: update vertex global data in update_store
}

void WCCApp::contract(VertexID src_id, VertexID dst_id, EdgeIndex idx) {
  if (graph_->ReadLocalVertexData(src_id)->p ==
      graph_->ReadLocalVertexData(dst_id)->p) {
    graph_->DeleteEdge(idx);
  }
}

void WCCApp::message_passing(VertexID id) {
  if (update_store_->Read(id)->p < graph_->ReadLocalVertexData(id)->p) {
    if (!graph_->IsInGraph(graph_->ReadLocalVertexData(id)->p)) {
      mtx.lock();
      id_to_p[graph_->ReadLocalVertexData(id)->p] = update_store_->Read(id)->p;
      mtx.unlock();
    } else {
      // TODO: active vertex update global info
      graph_->WriteLocalVertexData(graph_->ReadLocalVertexData(id)->p,
                                   WccVertexData(update_store_->Read(id)->p));
    }
  }
}

void WCCApp::point_jump_inc(VertexID id) {
  // is not in graph
  if (!graph_->IsInGraph(graph_->ReadLocalVertexData(id)->p)) {
    mtx.lock();
    id_to_p[graph_->ReadLocalVertexData(id)->p] =
        graph_->ReadLocalVertexData(id)->p;
    mtx.unlock();
  } else {
    bool flag = graph_->WriteLocalVertexData(
        id, WccVertexData(
                graph_->ReadLocalVertexData(graph_->ReadLocalVertexData(id)->p)
                    ->p));
    if (flag) {
      // TODO: active vertex update global info
    }
  }
}

}  // namespace sics::graph::core::apps