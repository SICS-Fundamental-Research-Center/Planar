#include "apps/sssp_app.h"

namespace sics::graph::core::apps {

void SsspApp::PEval() {
  auto init = [this](VertexID id) { this->Init(id); };
  auto relax = [this](VertexID id) { this->Relax(id); };

  //  graph_->LogGraphInfo();
  //  graph_->LogVertexData();
  ParallelVertexDo(init);
  LOGF_INFO("init finished, active: {}", active_.Count());
  //  graph_->LogVertexData();

  while (active_.Count() != 0) {
    ParallelVertexDo(relax);
    SyncActive();
    LOGF_INFO("relax finished, active: {}", active_.Count());
    //    graph_->LogVertexData();
  }
}

void SsspApp::IncEval() {
  auto message_passing = [this](VertexID id) { this->MessagePassing(id); };
  auto relax = [this](VertexID id) { this->Relax(id); };

  active_.Clear();
  //  update_store_->LogGlobalMessage();
  //  graph_->LogVertexData();
  ParallelVertexDo(message_passing);
  LOGF_INFO("message passing finished, active: {}", active_.Count());
  //  graph_->LogVertexData();

  while (active_.Count() != 0) {
    ParallelVertexDo(relax);
    SyncActive();
    LOGF_INFO("relax finished, active: {}", active_.Count());
    //    graph_->LogVertexData();
  }
}

void SsspApp::Assemble() {}

void SsspApp::Init(VertexID id) {
  if (id == source_) {
    graph_->WriteVertexDataByID(id, 0);
    update_store_->WriteMinBorderVertex(id, 0);
    active_.SetBit(id);
    LOGF_INFO("source of SSSP: {}", id);
  } else {
    graph_->WriteVertexDataByID(id, SSSP_INFINITY);
  }
}

void SsspApp::Relax(VertexID id) {
  if (active_.GetBit(id)) {
    // push to neighbors
    auto edges = graph_->GetOutEdgesByID(id);
    auto degree = graph_->GetOutDegreeByID(id);
    auto current_distance = graph_->ReadLocalVertexDataByID(id) + 1;
    for (int i = 0; i < degree; i++) {
      auto dst_id = edges[i];
      if (current_distance < graph_->ReadLocalVertexDataByID(dst_id)) {
        if (graph_->WriteMinVertexDataByID(dst_id, current_distance)) {
          update_store_->WriteMinBorderVertex(dst_id, current_distance);
          active_next_round_.SetBit(dst_id);
        }
      }
    }
  }
}

void SsspApp::MessagePassing(VertexID id) {
  if (graph_->WriteMinVertexDataByID(id, update_store_->Read(id)))
    active_.SetBit(id);
}

void SsspApp::SyncActive() {
  std::swap(active_, active_next_round_);
  active_next_round_.Init(update_store_->GetMessageCount());
}

}  // namespace sics::graph::core::apps
