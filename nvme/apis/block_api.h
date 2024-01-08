#ifndef GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_
#define GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_

#include <functional>
#include <type_traits>
#include <vector>

#include "core/common/multithreading/task_runner.h"
#include "core/common/types.h"
#include "core/data_structures/graph/pram_block.h"
#include "core/data_structures/serializable.h"
#include "core/util/logging.h"
#include "nvme/apis/block_api_base.h"
#include "nvme/scheduler/message_hub.h"
#include "nvme/scheduler/scheduler.h"
#include "nvme/update_stores/nvme_update_store.h"

namespace sics::graph::nvme::apis {

using core::data_structures::Serializable;
using Block32 = core::data_structures::graph::BlockCSRGraphUInt32;

class BlockModel : public BlockModelBase {
  using VertexData = typename Block32::VertexData;
  using EdgeData = typename Block32::EdgeData;

  using GraphID = core::common::GraphID;
  using VertexID = core::common::VertexID;
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;

  using ExecuteMessage = sics::graph::nvme::scheduler::ExecuteMessage;
  using ExecuteType = sics::graph::nvme::scheduler::ExecuteType;

 public:
  BlockModel() {}

  ~BlockModel() override = default;

  // ===============================================================
  // Map functions should use scheduler to iterate over all blocks
  // ===============================================================

  // init all data of blocks
  void Init() {
    // for different blocks, init different data
  }

  void MapVertex(const std::function<void(VertexID)>& vertex_func) {
    // all blocks should be executor the vertex function
    ExecuteMessage message;
    message.execute_type = ExecuteType::kCompute;
    scheduler_->RunMapVertex(message);
  }

  void MapEdge(const std::function<void(VertexID, VertexID)>& edge_func) {}

  void MapAndMutateEdge(
      const std::function<void(VertexID, VertexID, EdgeIndex)>& edge_del_func) {
  }

  // ===============================================================
  // Map functions should use scheduler to iterate over all blocks
  // ===============================================================

  VertexData Read(VertexID id) { update_store_->Read(id); }

  void Write(VertexID id, VertexData vdata) { update_store_->Write(id, vdata); }

  void WriteMin(VertexID id, VertexData vdata) {
    update_store_->WriteMin(id, vdata);
  }

  // methods for graph
  size_t GetGraphEdges() const { return scheduler_->GetGraphEdges(); }

 protected:
  core::common::TaskRunner* exe_runner_ = nullptr;

  // all blocks are stored in the GraphState of scheduler
  scheduler::PramScheduler* scheduler_ = nullptr;

  update_stores::BspUpdateStoreUInt32* update_store_ = nullptr;

  int round_ = 0;

  // configs
  const uint32_t parallelism_ = 10;
  const uint32_t task_package_factor_ = 10;
};

}  // namespace sics::graph::nvme::apis

#endif  // GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_
