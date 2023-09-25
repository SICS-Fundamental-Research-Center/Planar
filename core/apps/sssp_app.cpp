#include "apps/sssp_app.h"

namespace sics::graph::core::apps {

void SsspApp::PEval() {
  auto relax = [this](VertexID id) { this->Relax(id); };
  while (active_.Count() != 0) {
    ParallelVertexDo(relax);
  }
}

void SsspApp::IncEval() {
  auto message_passing = [this](VertexID id) { this->MessagePassing(id); };
  auto relax = [this](VertexID id) { this->Relax(id); };

  ParallelVertexDo(message_passing);

  while (active_.Count() != 0) {
    ParallelVertexDo(relax);
  }
}

void SsspApp::Assemble() {}

void SsspApp::Init(VertexID id) {
  if (id == 0) {
    graph_->WriteVertexDataByID(id, 0);
    active_.SetBit(id);
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
      if (current_distance < graph_->ReadLocalVertexDataByID(edges[i])) {
        if (graph_->WriteMinVertexDataByID(edges[i], current_distance)) {
          update_store_->WriteMin(edges[i], current_distance);
          active_.SetBit(edges[i]);
        }
      }
    }
    active_.ClearBit(id);
  }
}

void SsspApp::MessagePassing(VertexID id) {
  if (graph_->WriteMinVertexDataByID(id, update_store_->Read(id)))
    active_.SetBit(id);
}

}  // namespace sics::graph::core::apps
