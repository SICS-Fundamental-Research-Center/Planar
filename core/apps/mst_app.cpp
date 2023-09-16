#include "apps/mst_app.h"

namespace sics::graph::core::apps {

MstApp::MstApp(
    common::TaskRunner* runner,
    update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
    data_structures::Serializable* graph)
    : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {}

void MstApp::PEval() {}

void MstApp::IncEval() {}

void MstApp::Assemble() {}

}  // namespace sics::graph::core::apps