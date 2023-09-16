#include "apps/coloring_app.h"

namespace sics::graph::core::apps {

ColoringApp::ColoringApp(
    common::TaskRunner* runner,
    update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
    data_structures::Serializable* graph)
    : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {}

void ColoringApp::PEval() {}

void ColoringApp::IncEval() {}

void ColoringApp::Assemble() {}

}  // namespace sics::graph::core::apps