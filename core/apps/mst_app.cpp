#include "apps/mst_app.h"

namespace sics::graph::core::apps {

void MstApp::PEval() {
  auto init = [this](VertexID id) { Init(id); };
  auto find_min_edge = [this](VertexID id) { FindMinEdge(id); };
  ParallelVertexDo(init);
  LOG_INFO("init finished");
  //  graph_->LogVertexData();
  //  graph_->LogGraphInfo();
  ParallelVertexDo(find_min_edge);
  LOG_INFO("find_min_edge finished");
  //  LogMinOutEdgeId();
  update_store_->SetActive();
}

void MstApp::IncEval() {
  auto message_passing = [this](VertexID id) { MessagePassing(id); };
  auto graft = [this](VertexID src_id) { Graft(src_id); };
  auto point_jump = [this](VertexID id) { PointJump(id); };
  auto contract = [this](VertexID src_id, VertexID dst_id, EdgeIndex idx) {
    Contract(src_id, dst_id, idx);
  };
  auto update_min_edge = [this](VertexID src_id) { UpdateMinEdge(src_id); };
  auto point_jump_inc = [this](VertexID id) { PointJumpInc(id); };

  if (graph_->GetOutEdgeNums() == 0) {
    LOG_INFO("Second IncEval begin");
    ParallelVertexDo(message_passing);
    LOG_INFO("message passing finished");
    graph_->LogVertexData();

    // point jump inc
    ParallelVertexDo(point_jump_inc);
    LOG_INFO("point_jump_inc finished");

  } else {
    LOG_INFO("first IncEval begin");
    while (graph_->GetOutEdgeNums() != 0) {
      //      graph_->LogGraphInfo();
      graph_->LogEdges();
      LogMinOutEdgeId();
      ParallelVertexDo(graft);
      LOG_INFO("graft finished");
      graph_->LogVertexData();

      ParallelVertexDo(point_jump);
      LOG_INFO("point_jump finished");
      graph_->LogVertexData();

      ParallelEdgeMutateDo(contract);
      LOG_INFO("contract finished");
      graph_->LogVertexData();

      ParallelVertexDo(update_min_edge);
      LOGF_INFO("update_min_edge finished, left edges: {}",
                graph_->GetOutEdgeNums());
      graph_->LogVertexData();
    }
  }
}

void MstApp::Assemble() {}

// private methods:

void MstApp::Init(VertexID id) { graph_->WriteVertexDataByID(id, id); }

void MstApp::FindMinEdge(VertexID id) {
  auto degree = graph_->GetOutDegreeByID(id);
  if (degree != 0) {
    auto edges = graph_->GetOutEdgesByID(id);
    VertexID dst_id = edges[0];
    for (int i = 1; i < degree; i++) {
      dst_id = edges[i] < dst_id ? edges[i] : dst_id;
    }
    min_out_edge_id_[id] = dst_id;  // No conflict, write directly.
  }
}

void MstApp::MessagePassing(VertexID id) {
  if (update_store_->Read(id) < graph_->ReadLocalVertexDataByID(id)) {
    auto parent_id = graph_->ReadLocalVertexDataByID(id);
    auto new_data = update_store_->Read(id);
    if (!graph_->IsInGraph(parent_id)) {
      WriteMinAuxiliary(parent_id, new_data);
    } else {
      if (graph_->WriteMinVertexDataByID(parent_id, new_data))
        update_store_->WriteMinBorderVertex(parent_id, new_data);
    }
  }
}

void MstApp::Graft(VertexID src_id) {
  //  auto parent_id = graph_->ReadLocalVertexDataByID(src_id);
  //  if (parent_id == src_id) {
  auto dst_id = min_out_edge_id_[src_id];
  if (dst_id != MST_INVALID_VID) {
    if (graph_->IsInGraph(dst_id)) {
      auto src_parent_id = graph_->ReadLocalVertexDataByID(src_id);
      auto dst_parent_id = graph_->ReadLocalVertexDataByID(dst_id);
      if (src_parent_id < dst_parent_id) {
        if (graph_->WriteMinVertexDataByID(dst_parent_id, src_parent_id))
          update_store_->WriteMinBorderVertex(dst_parent_id, src_parent_id);
      } else {
        if (graph_->WriteMinVertexDataByID(src_parent_id, dst_parent_id))
          update_store_->WriteMinBorderVertex(src_parent_id, dst_parent_id);
      }
    } else {
      // not in graph dst_id
      auto src_parent_id = graph_->ReadLocalVertexDataByID(src_id);
      if (dst_id < src_parent_id) {
        if (graph_->WriteMinVertexDataByID(src_parent_id, dst_id))
          update_store_->WriteMinBorderVertex(src_parent_id, dst_id);
      }
    }
  }
  //  }
}

void MstApp::PointJump(VertexID id) {
  VertexData parent_id = graph_->ReadLocalVertexDataByID(id);
  if (graph_->IsInGraph(parent_id) &&
      parent_id != graph_->ReadLocalVertexDataByID(parent_id)) {
    while (parent_id != graph_->ReadLocalVertexDataByID(parent_id)) {
      parent_id = graph_->ReadLocalVertexDataByID(parent_id);
      if (!graph_->IsInGraph(parent_id)) break;
    }
    if (graph_->WriteMinVertexDataByID(id, parent_id)) {
      update_store_->WriteMinBorderVertex(id, parent_id);
    }
  }
}

void MstApp::Contract(VertexID src_id, VertexID dst_id, EdgeIndex idx) {
  if (graph_->ReadLocalVertexDataByID(src_id) ==
      graph_->ReadLocalVertexDataByID(dst_id)) {
    graph_->DeleteEdge(src_id, idx);
  }
}

void MstApp::UpdateMinEdge(VertexID id) {
  auto degree = graph_->GetOutDegreeByID(id);
  if (degree != 0) {
    auto edges = graph_->GetOutEdgesByID(id);
    VertexID dst_id = edges[0];
    for (int i = 1; i < degree; i++) {
      dst_id = edges[i] < dst_id ? edges[i] : dst_id;
    }
    //    auto parent_id = graph_->ReadLocalVertexDataByID(id);
    min_out_edge_id_[id] = dst_id;  // No conflict, write directly.
  }
}

void MstApp::PointJumpInc(VertexID id) {
  VertexData parent_id = graph_->ReadLocalVertexDataByID(id);
  if (!graph_->IsInGraph(parent_id)) {
    if (id_to_p_.find(parent_id) != id_to_p_.end())
      if (graph_->WriteMinVertexDataByID(id, id_to_p_[parent_id])) {
        update_store_->WriteMinBorderVertex(id, id_to_p_[parent_id]);
      }
  } else {
    auto tmp = graph_->ReadLocalVertexDataByID(parent_id);
    if (tmp != parent_id) {
      if (graph_->WriteMinVertexDataByID(id, tmp)) {
        update_store_->WriteMinBorderVertex(id, tmp);
      }
    }
  }
}

void MstApp::LogMinOutEdgeId() {
  for (int i = 0; i < update_store_->GetMessageCount(); i++) {
    LOGF_INFO("e(v) id: {} -> {}", i, min_out_edge_id_[i]);
  }
}

void MstApp::WriteMinAuxiliary(VertexID id, VertexData data) {
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