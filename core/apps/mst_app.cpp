#include "apps/mst_app.h"

namespace sics::graph::core::apps {

void MstApp::PEval() {
  auto init = [this](VertexID id) { Init(id); };
  auto find_min_edge = [this](VertexID id) { FindMinEdge(id); };
  ParallelVertexDo(init);
  ParallelVertexDo(find_min_edge);
}

void MstApp::IncEval() {
  auto message_passing = [this](VertexID id) { MessagePassing(id); };
  auto graft = [this](VertexID src_id) { Graft(src_id); };
  auto point_jump = [this](VertexID id) { PointJump(id); };
  auto contract = [this](VertexID src_id, VertexID dst_id, EdgeIndex idx) {
    Contract(src_id, dst_id, idx);
  };
  auto update_min_edge = [this](VertexID src_id) { UpdateMinEdge(src_id); };

  ParallelVertexDo(message_passing);
  ParallelVertexDo(graft);
  ParallelVertexDo(point_jump);
  ParallelEdgeMutateDo(contract);
  ParallelVertexDo(update_min_edge);
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
  // sync p(v)
  if (update_store_->Read(id) < graph_->ReadLocalVertexDataByID(id))
    graph_->WriteMinVertexDataByID(id, update_store_->Read(id));
}

void MstApp::Graft(VertexID src_id) {
  auto parent_id = graph_->ReadLocalVertexDataByID(src_id);
  if (parent_id == src_id) {
    auto dst_id = min_out_edge_id_[src_id];
    if (graph_->IsInGraph(dst_id)) {
      auto src_parent_id = graph_->ReadLocalVertexDataByID(src_id);
      auto dst_parent_id = graph_->ReadLocalVertexDataByID(dst_id);
      if (src_parent_id < dst_parent_id) {
        if (graph_->WriteMinVertexDataByID(dst_parent_id, src_parent_id))
          update_store_->WriteMin(dst_parent_id, src_parent_id);
      } else {
        if (graph_->WriteMinVertexDataByID(src_parent_id, dst_parent_id))
          update_store_->WriteMin(src_parent_id, dst_parent_id);
      }
    } else {
      // not in graph dst_id
      auto src_parent_id = graph_->ReadLocalVertexDataByID(src_id);
      if (dst_id < src_parent_id) {
        if (graph_->WriteMinVertexDataByID(src_parent_id, dst_id))
          update_store_->WriteMin(src_parent_id, dst_id);
      }
    }
  }
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
      update_store_->WriteMin(id, parent_id);
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
    min_out_edge_id_[id] = dst_id;  // No conflict, write directly.
  }
}

}  // namespace sics::graph::core::apps