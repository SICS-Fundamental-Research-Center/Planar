#include "nvme/apps/wcc_nvme_app.h"

namespace sics::graph::nvme::apps {

WCCNvmeApp::WCCNvmeApp(
    core::common::TaskRunner* runner,
    core::update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
    core::data_structures::Serializable* graph)
    : core::apis::PlanarAppBase<BlockGraph>(runner, update_store, graph) {}

void WCCNvmeApp::PEval() {}

void WCCNvmeApp::IncEval() {}

void WCCNvmeApp::Assemble() {}

}  // namespace sics::graph::nvme::apps