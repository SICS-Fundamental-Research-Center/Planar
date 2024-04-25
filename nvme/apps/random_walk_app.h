#ifndef GRAPH_SYSTEMS_NVME_APPS_RANDOM_WALK_APP_H_
#define GRAPH_SYSTEMS_NVME_APPS_RANDOM_WALK_APP_H_

#include "core/apis/planar_app_base.h"
#include "nvme/apis/block_api.h"
#include "nvme/data_structures/graph/pram_block.h"

namespace sics::graph::nvme::apps {

using BlockGraph = data_structures::graph::BlockCSRGraphUInt32;
using VertexID = core::common::VertexID;

#define WALK_LENGTH 5

struct Data {
  VertexID next_id[5];
};

class RandomWalkApp : public apis::BlockModel<BlockGraph::VertexData> {
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexID = core::common::VertexID;

  using FuncVertex = core::common::FuncVertex;
  using FuncEdge = core::common::FuncEdge;
  using FuncEdgeAndMutate = core::common::FuncEdgeAndMutate;
  using FuncEdgeMutate = core::common::FuncEdgeMutate;

 public:
  RandomWalkApp() = default;
  RandomWalkApp(const std::string& root_path) : BlockModel(root_path) {
    matrix_ = new VertexID[scheduler_.GetVertexNumber() * WALK_LENGTH];
  }
  ~RandomWalkApp() override = default;

  void Init(apps::VertexID id) {}

  void Sample(VertexID id) {}

  void Walk(VertexID id) {}

 private:
  VertexID* GetRoad(VertexID id) {
    uint64_t index = (uint64_t)(id)*WALK_LENGTH;
    return matrix_ + index;
  }

 private:
  VertexID* matrix_ = nullptr;
};

}  // namespace sics::graph::nvme::apps

#endif  // GRAPH_SYSTEMS_NVME_APPS_RANDOM_WALK_APP_H_
