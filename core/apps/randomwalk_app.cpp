#include "apps/randomwalk_app.h"

namespace xyz::graph::core::apps {

RandomWalkApp::RandomWalkApp(
    common::TaskRunner* runner,
    update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
    data_structures::Serializable* graph)
    : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {}

void RandomWalkApp::PEval() {
  auto sample = [this](VertexID id) { this->Sample(id); };
  ParallelVertexDo(sample);
  LOG_INFO("Sample finished");
  update_store_->SetActive();
}

void RandomWalkApp::IncEval() {
  auto walk = [this](VertexID id) { this->Walk(id); };
  ParallelVertexDo(walk);
  LOG_INFO("Walk finished");
  update_store_->UnsetActive();
}

void RandomWalkApp::Assemble() {}

void RandomWalkApp::Sample(VertexID id) {
  auto degree = graph_->GetOutDegreeByID(id);
  for (int i = 0; i < walk_length_; i++) {
    if (degree == 0) {
      GetRoad(id)[i] = id;
    } else {
      GetRoad(id)[i] = graph_->GetOutEdgesByID(id)[GetRandom(degree)];
    }
  }
}

void RandomWalkApp::Walk(VertexID id) {
  auto tmp = id;
  for (int i = 0; i < walk_length_; i++) {
    tmp = GetRoad(tmp)[i];
  }
}

}  // namespace xyz::graph::core::apps
