#ifndef GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_
#define GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_

#include <functional>
#include <type_traits>
#include <vector>

#include "core/common/multithreading/task_runner.h"
#include "core/common/types.h"
#include "core/data_structures/serializable.h"
#include "core/util/logging.h"
#include "nvme/apis/block_api_base.h"
#include "nvme/scheduler/message_hub.h"
#include "nvme/scheduler/scheduler.h"

namespace sics::graph::nvme::apis {

using core::data_structures::Serializable;

template <typename GraphType>
class BlockModel : public BlockModelBase {
  static_assert(std::is_base_of<Serializable, GraphType>::value,
                "GraphType must be a subclass of Serializable");
  using VertexData = typename GraphType::VertexData;
  using EdgeData = typename GraphType::EdgeData;

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

 protected:
  core::common::TaskRunner* exe_runner_ = nullptr;

  GraphType* graph_ = nullptr;

  scheduler::PramScheduler* scheduler_ = nullptr;

  int round_ = 0;

  // configs
  const uint32_t parallelism_ = 10;
};

}  // namespace sics::graph::nvme::apis

#endif  // GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_
