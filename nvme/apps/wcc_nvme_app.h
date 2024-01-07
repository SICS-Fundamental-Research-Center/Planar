#ifndef GRAPH_SYSTEMS_NVME_APPS_WCC_NVME_APP_H_
#define GRAPH_SYSTEMS_NVME_APPS_WCC_NVME_APP_H_

#include "core/apis/planar_app_base.h"
#include "core/data_structures/graph/pram_block.h"
#include "nvme/apis/block_api.h"

namespace sics::graph::nvme::apps {

using BlockGraph = core::data_structures::graph::BlockCSRGraphUInt32;

class WCCNvmeApp : public apis::BlockModel<BlockGraph> {
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexID = core::common::VertexID;

 public:
  using VertexData = typename BlockGraph ::VertexData;
  using EdgeData = typename BlockGraph ::EdgeData;
  WCCNvmeApp() = default;
  ~WCCNvmeApp() override = default;

  void Init(VertexID id) {}
  void Graft(VertexID src_id, VertexID dst_id) {}
  void PointJump(VertexID src_id) {}
  void Contract(VertexID src_id, VertexID dst_id, EdgeIndex idx) {}

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
      if (graph_->GetOutEdgeNums() == 0) {
        break;
      }
    }
  }

 private:
};

// main function for wcc nvme app

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_bool(in_memory, false, "in memory mode");
DEFINE_uint32(memory_size, 64, "memory size (GB)");
DEFINE_uint32(limits, 0, "subgrah limits for pre read");
DEFINE_bool(no_short_cut, false, "no short cut");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  core::common::Configurations::GetMutable()->root_path = FLAGS_i;
  core::common::Configurations::GetMutable()->parallelism = FLAGS_p;
  core::common::Configurations::GetMutable()->in_memory = FLAGS_in_memory;
  core::common::Configurations::GetMutable()->limits = FLAGS_limits;
  // wcc nvme specific configurations
  core::common::Configurations::GetMutable()->is_block_mode = true;

  WCCNvmeApp app;
  app.Compute();
  return 0;
}

}  // namespace sics::graph::nvme::apps
#endif  // GRAPH_SYSTEMS_NVME_APPS_WCC_NVME_APP_H_
