#include "apps/wcc_app.h"

namespace sics::graph::core::apps {

// Note: check whether the app is registered.
WCCApp::WCCApp(
    common::TaskRunner* runner,
    update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
    data_structures::Serializable* graph)
    : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {}

void WCCApp::PEval() {
  auto init = [this](VertexID id) { this->Init(id); };
  auto graft = [this](VertexID src_id, VertexID dstId) {
    this->Graft(src_id, dstId);
  };
  auto point_jump = [this](VertexID id) { this->PointJump(id); };
  auto contract = [this](VertexID src_id, VertexID dst_id,
                         EdgeIndex edge_index) {
    this->Contract(src_id, dst_id, edge_index);
  };
  LOG_INFO("PEval begin");
  //  update_store_->LogBorderVertexInfo();
  //  graph_->LogGraphInfo();
  //  graph_->LogEdges();
  //  graph_->LogVertexData();
  //  graph_->LogIndexInfo();
  ParallelVertexDo(init);
  //  graph_->LogVertexData();
  LOG_INFO("init finished");
  while (graph_->GetOutEdgeNums() != 0) {
    //    update_store_->LogGlobalMessage();
    ParallelEdgeDo(graft);
    LOG_INFO("graft finished");
    //    graph_->LogVertexData();
    //    update_store_->LogGlobalMessage();

    ParallelVertexDo(point_jump);
    LOG_INFO("pointjump finished");
    //    graph_->LogVertexData();
    //    update_store_->LogGlobalMessage();
    //    graph_->LogEdges();
    ParallelEdgeMutateDo(contract);
    LOG_INFO("contract finished");
    //    graph_->LogEdges();
    //    graph_->LogGraphInfo();
    //    graph_->LogVertexData();
  }
  LOG_INFO("PEval finished");
}

void WCCApp::IncEval() {
  auto message_passing = [this](VertexID id) { this->MessagePassing(id); };
  auto point_jump_inc_eval = [this](VertexID id) {
    this->PointJumpIncEval(id);
  };
  //  graph_->LogGraphInfo();
  //  graph_->LogVertexData();
  //  update_store_->LogGlobalMessage();
  //  graph_->LogIsIngraphInfo();
  ParallelVertexDo(message_passing);
  LOG_INFO("message passing finished");
  //  graph_->LogVertexData();
  //  update_store_->LogGlobalMessage();

  ParallelVertexDo(point_jump_inc_eval);
  LOG_INFO("point_jump increment finished");
  //  graph_->LogVertexData();
  //  update_store_->LogGlobalMessage();

  graph_->set_status("IncEval");
}

void WCCApp::Assemble() { graph_->set_status("Assemble"); }

void WCCApp::Init(VertexID id) { graph_->WriteVertexDataByID(id, id); }

void WCCApp::Graft(VertexID src_id, VertexID dst_id) {
  VertexID src_parent_id = graph_->ReadLocalVertexDataByID(src_id);
  VertexID dst_parent_id = graph_->ReadLocalVertexDataByID(dst_id);
  if (src_parent_id < dst_parent_id) {
    if (graph_->WriteMinVertexDataByID(dst_parent_id, src_parent_id))
      update_store_->WriteMin(dst_parent_id, src_parent_id);
  } else if (src_parent_id > dst_parent_id) {
    if (graph_->WriteMinVertexDataByID(src_parent_id, dst_parent_id))
      update_store_->WriteMin(src_parent_id, dst_parent_id);
  }
}

// TODO: delete when test finished
void WCCApp::Graft2(VertexID src_id, VertexID dst_id) {
  VertexID src_parent_id =
      graph_->vertex_data_read_base_[graph_->index_by_global_id_[src_id]];
  VertexID dst_parent_id =
      graph_->vertex_data_read_base_[graph_->index_by_global_id_[dst_id]];
  if (src_parent_id < dst_parent_id) {
    auto data_new = graph_->vertex_data_read_base_
                        [graph_->index_by_global_id_[src_parent_id]];
    if (graph_->WriteMinVertexDataByID(dst_parent_id, data_new))
      update_store_->WriteMin(dst_parent_id, data_new);
  } else if (src_parent_id > dst_parent_id) {
    auto data_new = graph_->vertex_data_read_base_
                        [graph_->index_by_global_id_[dst_parent_id]];
    if (graph_->WriteMinVertexDataByID(src_parent_id, data_new))
      update_store_->WriteMin(src_parent_id, data_new);
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
  VertexData parent_id = graph_->ReadLocalVertexDataByID(id);
  if (!graph_->IsInGraph(parent_id)) {
    std::lock_guard<std::mutex> grd(mtx_);
    if (id_to_p_.find(parent_id) != id_to_p_.end())
      if (graph_->WriteMinVertexDataByID(id, id_to_p_[parent_id])) {
        update_store_->WriteMin(id, id_to_p_[parent_id]);
      }
  } else {
    auto tmp = graph_->ReadLocalVertexDataByID(parent_id);
    if (tmp != parent_id) {
      if (graph_->WriteMinVertexDataByID(id, tmp)) {
        update_store_->WriteMin(id, tmp);
      }
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