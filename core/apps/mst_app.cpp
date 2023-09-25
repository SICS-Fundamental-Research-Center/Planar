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

// private methods:

void MstApp::Init(VertexID id) { graph_->WriteVertexDataByID(id, id); }

void MstApp::Graft(VertexID src_id) {
  if (graph_->GetOutDegreeByID(src_id) != 0) {
    auto oneEdge = graph_->GetOneOutEdge(src_id, 0);

  }
}

void MstApp::MessagePassing(VertexID id) {}

void MstApp::PointJump(VertexID id) {}

void MstApp::Contract(VertexID src_id, VertexID dst_id, EdgeIndex idx) {}

}  // namespace sics::graph::core::apps