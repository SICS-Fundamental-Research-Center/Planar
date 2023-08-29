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

void WCCApp::init(VertexID id) { graph_->WriteLocalVertexData(id, id); }

void WCCApp::graft(VertexID src_id, VertexID dst_id) {
  VertexID src_parent_id = graph_->ReadLocalVertexDataByID(src_id);
  VertexID dst_parent_id = graph_->ReadLocalVertexDataByID(dst_id);
  if (src_parent_id != dst_parent_id) {
    if (src_parent_id < dst_parent_id) {
      graph_->WriteLocalVertexData(
          dst_parent_id, graph_->ReadLocalVertexDataByID(src_parent_id));
    } else {
      graph_->WriteLocalVertexData(
          src_parent_id, graph_->ReadLocalVertexDataByID(dst_parent_id));
    }
  }
  // TODO: add UpdateStore info logic
}

void WCCApp::point_jump(VertexID src_id) {
  VertexData parent_id = graph_->ReadLocalVertexDataByID(src_id);
  if (parent_id != graph_->ReadLocalVertexDataByID(parent_id)) {
    while (parent_id != graph_->ReadLocalVertexDataByID(parent_id)) {
      parent_id = graph_->ReadLocalVertexDataByID(parent_id);
    }
    graph_->WriteLocalVertexData(src_id, parent_id);
  }
  // TODO: update vertex global data in update_store
}

void WCCApp::contract(VertexID src_id, VertexID dst_id, EdgeIndex idx) {
  if (graph_->ReadLocalVertexDataByID(src_id) ==
      graph_->ReadLocalVertexDataByID(dst_id)) {
    graph_->DeleteEdge(idx);
  }
}

void WCCApp::message_passing(VertexID id) {
  if (update_store_->Read(id) < graph_->ReadLocalVertexDataByID(id)) {
    if (!graph_->IsInGraph(graph_->ReadLocalVertexDataByID(id))) {
      mtx.lock();
      id_to_p[graph_->ReadLocalVertexDataByID(id)] = update_store_->Read(id);
      mtx.unlock();
    } else {
      // TODO: active vertex update global info
      graph_->WriteLocalVertexData(graph_->ReadLocalVertexDataByID(id),
                                   update_store_->Read(id));
    }
  }
}

void WCCApp::point_jump_inc(VertexID id) {
  // is not in graph
  if (!graph_->IsInGraph(graph_->ReadLocalVertexDataByID(id))) {
    mtx.lock();
    id_to_p[graph_->ReadLocalVertexDataByID(id)] =
        graph_->ReadLocalVertexDataByID(id);
    mtx.unlock();
  } else {
    bool flag = graph_->WriteLocalVertexData(
        id,
        graph_->ReadLocalVertexDataByID(graph_->ReadLocalVertexDataByID(id)));
    if (flag) {
      // TODO: active vertex update global info
    }
  }
}

}  // namespace sics::graph::core::apps