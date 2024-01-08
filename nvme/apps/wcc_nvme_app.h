#ifndef GRAPH_SYSTEMS_NVME_APPS_WCC_NVME_APP_H_
#define GRAPH_SYSTEMS_NVME_APPS_WCC_NVME_APP_H_

#include "core/apis/planar_app_base.h"
#include "core/data_structures/graph/pram_block.h"
#include "nvme/apis/block_api.h"

namespace sics::graph::nvme::apps {

using BlockGraph = core::data_structures::graph::BlockCSRGraphUInt32;

class WCCNvmeApp : public apis::BlockModel {
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexID = core::common::VertexID;

 public:
  using VertexData = typename BlockGraph ::VertexData;
  using EdgeData = typename BlockGraph ::EdgeData;
  WCCNvmeApp() = default;
  ~WCCNvmeApp() override = default;

  // actually, this is no use.
  void Init(VertexID id) { Write(id, id); }

  void Graft(VertexID src_id, VertexID dst_id) {}
  void PointJump(VertexID src_id) {}
  void Contract(VertexID src_id, VertexID dst_id, EdgeIndex idx) {}

  // TODO:
  // delete pointer 'this' in anonymous namespace
  void Compute() override {
    auto init = [this](VertexID id) { Init(id); };
    auto graft = [this](VertexID src_id, VertexID dst_id) {
      Graft(src_id, dst_id);
    };
    auto point_jump = [this](VertexID src_id) { PointJump(src_id); };
    auto contract = [this](VertexID src_id, VertexID dst_id, EdgeIndex idx) {
      Contract(src_id, dst_id, idx);
    };

    MapVertex(init);

    while (true) {
      MapEdge(graft);
      MapVertex(point_jump);
      MapAndMutateEdge(contract);
      if (GetGraphEdges() == 0) {
        break;
      }
    }
  }

 private:
};

}  // namespace sics::graph::nvme::apps
#endif  // GRAPH_SYSTEMS_NVME_APPS_WCC_NVME_APP_H_
