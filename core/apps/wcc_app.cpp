#include "apps/wcc_app.h"

namespace sics::graph::core::apps {

// Note: check whether the app is registered.
WCCApp::WCCApp(
    common::TaskRunner* runner,
    update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
    data_structures::Serializable* graph)
    : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {}

void WCCApp::PEval() {
  graph_->LogGraphInfo();
  graph_->LogVertexData();
  ParallelVertexDo(std::bind(&WCCApp::Init, this, std::placeholders::_1));
  graph_->LogVertexData();
  while (graph_->GetOutEdgeNums() != 0) {
    ParallelEdgeDo(std::bind(&WCCApp::Graft, this, std::placeholders::_1,
                             std::placeholders::_2));
    graph_->LogVertexData();

    ParallelVertexDo(
        std::bind(&WCCApp::PointJump, this, std::placeholders::_1));

    graph_->LogVertexData();

    graph_->LogEdges();
    ParallelEdgeMutateDo(std::bind(&WCCApp::Contract, this,
                                   std::placeholders::_1, std::placeholders::_2,
                                   std::placeholders::_3));
    graph_->LogEdges();
    graph_->LogGraphInfo();
    graph_->LogVertexData();
  }
  graph_->set_status("PEval");
}

void WCCApp::IncEval() {
  ParallelVertexDo(
      std::bind(&WCCApp::MessagePassing, this, std::placeholders::_1));

  ParallelVertexDo(
      std::bind(&WCCApp::PointJumpIncEval, this, std::placeholders::_1));

  graph_->set_status("IncEval");
}

void WCCApp::Assemble() { graph_->set_status("Assemble"); }

void WCCApp::Init(VertexID id) { graph_->WriteVertexDataByID(id, id); }

// TODO: maybe update_store_ can check bitmap first when write_min
void WCCApp::Graft(VertexID src_id, VertexID dst_id) {
  VertexID src_parent_id = graph_->ReadLocalVertexDataByID(src_id);
  VertexID dst_parent_id = graph_->ReadLocalVertexDataByID(dst_id);
  if (src_parent_id < dst_parent_id) {
    if (graph_->WriteMinVertexDataByID(
            dst_parent_id, graph_->ReadLocalVertexDataByID(src_parent_id)))
      update_store_->WriteMin(dst_parent_id,
                              graph_->ReadLocalVertexDataByID(src_parent_id));
  } else if (src_parent_id > dst_parent_id) {
    if (graph_->WriteMinVertexDataByID(
            src_parent_id, graph_->ReadLocalVertexDataByID(dst_parent_id)))
      update_store_->WriteMin(src_parent_id,
                              graph_->ReadLocalVertexDataByID(dst_parent_id));
  }
}

void WCCApp::PointJump(VertexID src_id) {
  VertexData parent_id = graph_->ReadLocalVertexDataByID(src_id);
  if (parent_id != graph_->ReadLocalVertexDataByID(parent_id)) {
    while (parent_id != graph_->ReadLocalVertexDataByID(parent_id)) {
      parent_id = graph_->ReadLocalVertexDataByID(parent_id);
    }
    if (graph_->WriteMinVertexDataByID(src_id, parent_id)) {
      update_store_->WriteMin(src_id, parent_id);
    }
  }
}

void WCCApp::Contract(VertexID src_id, VertexID dst_id, EdgeIndex idx) {
  if (graph_->ReadLocalVertexDataByID(src_id) ==
      graph_->ReadLocalVertexDataByID(dst_id)) {
    graph_->DeleteEdge(src_id, idx);
  }
}

void WCCApp::MessagePassing(VertexID id) {
  if (update_store_->Read(id) < graph_->ReadLocalVertexDataByID(id)) {
    VertexData parent_id = graph_->ReadLocalVertexDataByID(id);
    VertexData new_data = update_store_->Read(id);
    if (!graph_->IsInGraph(parent_id)) {
      WriteMinAuxiliary(parent_id, new_data);
    } else {
      if (graph_->WriteMinVertexDataByID(parent_id, new_data)) {
        update_store_->WriteMin(parent_id, new_data);
      }
    }
  }
}

void WCCApp::PointJumpIncEval(VertexID id) {
  // is not in graph
  if (!graph_->IsInGraph(graph_->ReadLocalVertexDataByID(id))) {
    std::lock_guard<std::mutex> grd(mtx_);
    id_to_p_[graph_->ReadLocalVertexDataByID(id)] =
        graph_->ReadLocalVertexDataByID(id);
  } else {
    if (graph_->WriteMinVertexDataByID(
            id, graph_->ReadLocalVertexDataByID(
                    graph_->ReadLocalVertexDataByID(id)))) {
//      update_store_->WriteMin(id, );
    }
  }
}

void WCCApp::WriteMinAuxiliary(VertexID id, VertexData data) {
  std::lock_guard<std::mutex> grd(mtx_);
  if (id_to_p_.find(id) == id_to_p_.end()) {
    id_to_p_[id] = data;
  } else {
    if (id_to_p_[id] > data) {
      id_to_p_[id] = data;
    }
  }
}

}  // namespace sics::graph::core::apps