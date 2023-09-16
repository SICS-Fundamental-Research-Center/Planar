#include "apps/sssp_app.h"

namespace sics::graph::core::apps {

SsspApp::SsspApp(
    common::TaskRunner* runner,
    update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
    data_structures::Serializable* graph)
    : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {}

void SsspApp::PEval() {}

void SsspApp::IncEval() {}

void SsspApp::Assemble() {}

}  // namespace sics::graph::core::apps
