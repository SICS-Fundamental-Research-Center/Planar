#ifndef GRAPH_SYSTEMS_CORE_APPS_COLORING_APP_OP_H
#define GRAPH_SYSTEMS_CORE_APPS_COLORING_APP_OP_H

#include <cstdlib>
#include <random>

#include "apis/planar_app_base_op.h"
#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

class ColoringAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

 public:
  ColoringAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}
  ~ColoringAppOp() override = default;

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub) override {
    apis::PlanarAppBaseOp<uint32_t>::AppInit(runner, meta, buffer, graphs, hub);
  }

  void PEval() final {

  }
  void IncEval() final {

  }
  void Assemble() final {

  }

 private:
  void Init(VertexID id);

  void FindConflictsVertex(VertexID src_id, VertexID dst_id);

  void FindConflictsColor(VertexID src_id, VertexID dst_id);

  void Color(VertexID id);

  void MessagePassing(VertexID id);

  void ColorEdge(VertexID src_id, VertexID dst_id);

  void ColorVertex(VertexID id);

  int GetRandomNumber() const;

 private:
  int app_active_ = 0;
  //  common::Bitmap bitmap_;
  int round_ = 0;
  // configs
  uint32_t max_round_ = 10000;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_COLORING_APP_OP_H
