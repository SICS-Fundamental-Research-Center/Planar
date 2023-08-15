#include "apps/sample_app.h"

#include "apis/planar_app_factory.h"

namespace sics::graph::core::apps {

// Note: all PIE apps must register the app factory with this line.
bool SampleApp::registered_ = apis::PlanarAppFactory<DummyGraph>::Register(
    GetAppName(), &SampleApp::Create);

// Note: check whether the app is registered.
SampleApp::SampleApp(common::TaskRunner* runner,
                     data_structures::Serializable* graph)
    : apis::PlanarAppBase<DummyGraph>(runner, graph) {
  if (!SampleApp::registered_) {
    LOG_FATAL("SampleApp is not registered to the factory");
  }
}

void SampleApp::PEval() { graph_->set_status("PEval"); }

void SampleApp::IncEval() { graph_->set_status("IncEval"); }

void SampleApp::Assemble() { graph_->set_status("Assemble"); }

DummyGraph::DummyGraph() : status_("initialized") {}

std::unique_ptr<data_structures::Serialized> DummyGraph::Serialize(
    const common::TaskRunner& runner) {
  status_ = "serialized";
  return nullptr;
}

void DummyGraph::Deserialize(
    const common::TaskRunner& runner,
    std::unique_ptr<data_structures::Serialized>&& serialized) {
  status_ = "deserialized";
}

}  // namespace sics::graph::core::apps