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
#include "nvme/components/discharge.h"
#include "nvme/components/executor.h"
#include "nvme/components/loader.h"
#include "nvme/io/pram_block_reader.h"
#include "nvme/io/pram_block_writer.h"
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
  using MapType = sics::graph::nvme::scheduler::MapType;

 public:
  BlockModel() = default;
  BlockModel(const std::string& root_path) {
    scheduler_ = std::make_unique<scheduler::PramScheduler>(root_path);
    update_store_ = std::make_unique<update_stores::BspUpdateStoreUInt32>(
        scheduler_->GetVertexNumber(), scheduler_->GetEdgeNumber());
    loader_ = std::make_unique<components::Loader<io::PramBlockReader>>(
        root_path, scheduler_->GetMessageHub());
    discharge_ = std::make_unique<components::Discharger<io::PramBlockWriter>>(
        root_path, scheduler_->GetMessageHub());
    executor_ =
        std::make_unique<components::Executor>(scheduler_->GetMessageHub());
  }

  ~BlockModel() override {
    Stop();
    end_time_ = std::chrono::system_clock::now();
    LOGF_INFO(" =========== Hole Runtime: {} s ===========",
              std::chrono::duration<double>(end_time_ - begin_time_).count());
  }

  // ===============================================================
  // Map functions should use scheduler to iterate over all blocks
  // ===============================================================

  // init all data of blocks
  // TODO:
  //  move init in update_store to here
  void Init() {
    // for different blocks, init different data
  }

  void MapVertex(const std::function<void(VertexID)>& func_vertex) {
    // all blocks should be executor the vertex function
    ExecuteMessage message;
    message.map_type = MapType::kMapVertex;
    message.func_vertex = func_vertex;
    scheduler_->RunMapExecute(message);
  }

  void MapEdge(const std::function<void(VertexID, VertexID)>& func_edge) {
    // all blocks should be executor the edge function
    ExecuteMessage message;
    message.map_type = MapType::kMapEdge;
    message.func_edge = func_edge;
    scheduler_->RunMapExecute(message);
  }

  void MapAndMutateEdge(
      const std::function<void(VertexID, VertexID, EdgeIndex)>& func_edge_del) {
    // all blocks should be executor the edge function
    ExecuteMessage message;
    message.map_type = MapType::kMapEdgeAndMutate;
    message.func_edge_mutate = func_edge_del;
    scheduler_->RunMapExecute(message);
  }

  void Run() {
    LOG_INFO("Start running");
    begin_time_ = std::chrono::system_clock::now();
    loader_->Start();
    discharge_->Start();
    executor_->Start();
    scheduler_->Start();
  }

  void Stop() {
    scheduler_->Stop();
    loader_->StopAndJoin();
    discharge_->StopAndJoin();
    executor_->StopAndJoin();
  }
  // ===============================================================
  // Map functions should use scheduler to iterate over all blocks
  // ===============================================================

  VertexData Read(VertexID id) { update_store_->Read(id); }

  void Write(VertexID id, VertexData vdata) { update_store_->Write(id, vdata); }

  void WriteMin(VertexID id, VertexData vdata) {
    update_store_->WriteMin(id, vdata);
  }

  void DeleteEdge(VertexID src, VertexID dst, EdgeIndex idx) {
    update_store_->DeleteEdge(idx);
  }

  // methods for graph
  size_t GetGraphEdges() const { return scheduler_->GetGraphEdges(); }

 protected:
  core::common::TaskRunner* exe_runner_ = nullptr;

  // all blocks are stored in the GraphState of scheduler
  std::unique_ptr<scheduler::PramScheduler> scheduler_;
  std::unique_ptr<components::Loader<io::PramBlockReader>> loader_;
  std::unique_ptr<components::Discharger<io::PramBlockWriter>> discharge_;
  std::unique_ptr<components::Executor> executor_;
  std::unique_ptr<update_stores::BspUpdateStoreUInt32> update_store_;

  int round_ = 0;

  std::chrono::time_point<std::chrono::system_clock> begin_time_;
  std::chrono::time_point<std::chrono::system_clock> end_time_;

  // configs
  const uint32_t parallelism_ = 10;
  const uint32_t task_package_factor_ = 10;
};

}  // namespace sics::graph::nvme::apis

#endif  // GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_
