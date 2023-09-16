#include "apps/randomwalk_app.h"

namespace sics::graph::core::apps {

RandomWalkApp::RandomWalkApp(
    common::TaskRunner* runner,
    update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
    data_structures::Serializable* graph)
    : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {}

void RandomWalkApp::PEval() {}

void RandomWalkApp::IncEval() {}

void RandomWalkApp::Assemble() {}

}  // namespace sics::graph::core::apps
