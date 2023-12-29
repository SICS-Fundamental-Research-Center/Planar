#ifndef GRAPH_SYSTEMS_CORE_APPS_WCC_NVME_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_WCC_NVME_APP_H_

#include "apis/planar_app_base.h"
#include "data_structures/graph/pram_block.h"

namespace sics::graph::core::apps {

using BlockGraph = core::data_structures::graph::BlockCSRGraphUInt32;

class WCCNvmeApp : public core::apis::PlanarAppBase<BlockGraph> {
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexID = core::common::VertexID;

 public:
  using VertexData = typename BlockGraph ::VertexData;
  using EdgeData = typename BlockGraph ::EdgeData;
  WCCNvmeApp() = default;
  explicit WCCNvmeApp(
      core::common::TaskRunner* runner,
      core::update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      core::data_structures::Serializable* graph);
  ~WCCNvmeApp() override = default;

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
};

}  // namespace sics::graph::core::apps
#endif  // GRAPH_SYSTEMS_CORE_APPS_WCC_NVME_APP_H_
