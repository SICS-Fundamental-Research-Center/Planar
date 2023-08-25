#include "apps/wcc_app.h"

namespace sics::graph::core::apps {

// Note: check whether the app is registered.
WCCApp::WCCApp(
    common::TaskRunner* runner,
    update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
    data_structures::Serializable* graph)
    : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {}

void WCCApp::PEval() {
  ParallelVertexDo(std::bind(&WCCApp::init, this, std::placeholders::_1));
  graph_->set_status("PEval");
}

void WCCApp::IncEval() {
  graph_->set_status("IncEval");
  ParallelEdgeDo(std::bind(&WCCApp::graft, this, std::placeholders::_1,
                           std::placeholders::_2));
  ParallelEdgeMutateDo(std::bind(&WCCApp::contract, this, std::placeholders::_1,
                                 std::placeholders::_2, std::placeholders::_3));
}

void WCCApp::Assemble() { graph_->set_status("Assemble"); }

TestGraph::TestGraph() : status_("initialized") {}

std::unique_ptr<data_structures::Serialized> TestGraph::Serialize(
    const common::TaskRunner& runner) {
  status_ = "serialized";
  return nullptr;
}

void TestGraph::Deserialize(
    const common::TaskRunner& runner,
    std::unique_ptr<data_structures::Serialized>&& serialized) {
  status_ = "deserialized";
}

}  // namespace sics::graph::core::apps