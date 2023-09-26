#include "apps/mst_app.h"

namespace sics::graph::core::apps {

void MstApp::PEval() {
  auto init = [this](VertexID id) { Init(id); };
  auto graft = [this](VertexID src_id) { Graft(src_id); };
  auto point_jump = [this](VertexID id) { PointJump(id); };
  auto contract = [this](VertexID src_id, VertexID dst_id, EdgeIndex idx) {
    Contract(src_id, dst_id, idx);
  };
  ParallelVertexDo(init);
}

void MstApp::IncEval() {
  auto message_passing = [this](VertexID id) { MessagePassing(id); };
  auto graft = [this](VertexID src_id) { Graft(src_id); };
  auto point_jump = [this](VertexID id) { PointJump(id); };
  auto contract = [this](VertexID src_id, VertexID dst_id, EdgeIndex idx) {
    Contract(src_id, dst_id, idx);
  };

  ParallelVertexDo(message_passing);
  while (graph_->GetOutEdgeNums() != 0) {
    ParallelVertexDo(graft);
    ParallelVertexDo(point_jump);
    ParallelEdgeMutateDo(contract);
  }
}

void MstApp::Assemble() {}

// private methods:

void MstApp::Init(VertexID id) { graph_->WriteVertexDataByID(id, id); }

void MstApp::FindMinEdge(VertexID id) {

}

void MstApp::Graft(VertexID src_id) {
  if (graph_->GetOutDegreeByID(src_id) != 0) {
    auto dst_id = graph_->GetOneOutEdge(src_id, 0);
    auto src_parent_id = graph_->ReadLocalVertexDataByID(src_id);
    auto dst_parent_id = graph_->ReadLocalVertexDataByID(dst_id);
    if (src_parent_id < dst_parent_id) {
      if (graph_->WriteMinVertexDataByID(dst_parent_id, src_parent_id))
        update_store_->WriteMin(dst_parent_id, src_parent_id);
    } else {
      if (graph_->WriteMinVertexDataByID(src_parent_id, dst_parent_id))
        update_store_->WriteMin(src_parent_id, dst_parent_id);
    }
  }
}

void MstApp::MessagePassing(VertexID id) {
  graph_->WriteMinVertexDataByID(id, update_store_->Read(id));
}

void MstApp::PointJump(VertexID id) {
  VertexData parent_id = graph_->ReadLocalVertexDataByID(id);
  if (parent_id != graph_->ReadLocalVertexDataByID(parent_id)) {
    while (parent_id != graph_->ReadLocalVertexDataByID(parent_id)) {
      parent_id = graph_->ReadLocalVertexDataByID(parent_id);
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

}  // namespace sics::graph::core::apps