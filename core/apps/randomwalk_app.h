#ifndef GRAPH_SYSTEMS_CORE_APPS_RANDOMWALK_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_RANDOMWALK_APP_H_

#include "apis/planar_app_base.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

using CSRGraph = data_structures::graph::MutableCSRGraphUInt16;

class RandomWalkApp : public apis::PlanarAppBase<CSRGraph> {
  using VertexID = common::VertexID;

 public:
  using VertexData = CSRGraph::VertexData;
  using EdgeData = CSRGraph::EdgeData;
  RandomWalkApp() = default;
  explicit RandomWalkApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph);

  ~RandomWalkApp() override = default;

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
  void Init(VertexID id);

 private:
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_RANDOMWALK_APP_H_
