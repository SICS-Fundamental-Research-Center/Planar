#include "apps/sample_app.h"

namespace sics::graph::core::apps {

// Note: all PIE apps must register the app factory with this line.
bool SampleApp::registered_ = apis::PIEAppFactory<DummyGraph>::Register(
    GetAppName(), &SampleApp::Create);

// Note: check whether the app is registered.
SampleApp::SampleApp(common::TaskRunner* runner)
    : apis::PIE<DummyGraph>(runner) {
  if (!SampleApp::registered_) {
    LOGF_FATAL("SampleApp is not registered to the factory");
  }
}

void SampleApp::PEval(sics::graph::core::apps::DummyGraph* graph) {
  graph->set_status("PEval");
}

void SampleApp::IncEval(sics::graph::core::apps::DummyGraph* graph) {
  graph->set_status("IncEval");
}

void SampleApp::Assemble(sics::graph::core::apps::DummyGraph* graph) {
  graph->set_status("Assemble");
}

DummyGraph::DummyGraph() : status_("initialized") {}

std::unique_ptr<data_structures::Serialized>
DummyGraph::Serialize(const common::TaskRunner& runner) {
  status_ = "serialized";
  return nullptr;
}

void DummyGraph::Deserialize(
    const common::TaskRunner& runner,
    std::unique_ptr<data_structures::Serialized>&& serialized) {
  status_ = "deserialized";
}


}  // namespace sics::graph::core::apps