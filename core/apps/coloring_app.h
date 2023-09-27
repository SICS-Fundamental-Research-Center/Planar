#ifndef GRAPH_SYSTEMS_CORE_APPS_COLORING_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_COLORING_APP_H_

#include <cstdlib>
#include <random>

#include "apis/planar_app_base.h"
#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

using CSRGraph = data_structures::graph::MutableCSRGraphUInt32;

class ColoringApp : public apis::PlanarAppBase<CSRGraph> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;

 public:
  using VertexData = CSRGraph::VertexData;
  using EdgeData = CSRGraph::EdgeData;
  ColoringApp() = default;
  explicit ColoringApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph)
      : apis::PlanarAppBase<CSRGraph>(runner, update_store, graph) {
    bitmap_.Init(update_store_->GetMessageCount());
  }
  ~ColoringApp() override = default;

  void AppInit(common::TaskRunner* runner,
               update_stores::BspUpdateStore<VertexData, EdgeData>*
                   update_store) override {
    apis::PlanarAppBase<CSRGraph>::AppInit(runner, update_store);
    //    bitmap_.Init(update_store->GetMessageCount());
    srand(0);
    max_round_ = common::Configurations::Get()->rand_max;
  }

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
  void Init(VertexID id);

  void FindConflictsVertex(VertexID src_id, VertexID dst_id);

  void FindConflictsColor(VertexID src_id, VertexID dst_id);

  void Color(VertexID id);

  void MessagePassing(VertexID id);

  void ColorEdge(VertexID src_id, VertexID dst_id);

  void ColorVertex(VertexID id);

  int GetRandomNumber();

 private:
  int active_ = 0;
  common::Bitmap bitmap_;

  // configs
  uint32_t max_round_ = 10000;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_COLORING_APP_H_
