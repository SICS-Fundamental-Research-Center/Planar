#ifndef GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_
#define GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_

#include <condition_variable>
#include <functional>
#include <mutex>
#include <type_traits>
#include <vector>

#include "core/common/multithreading/task_runner.h"
#include "core/common/types.h"
#include "core/data_structures/serializable.h"
#include "core/util/logging.h"
#include "nvme/apis/block_api_base.h"
#include "nvme/components/discharge.h"
#include "nvme/components/executor.h"
#include "nvme/components/loader.h"
#include "nvme/data_structures/graph/pram_block.h"
#include "nvme/io/pram_block_reader.h"
#include "nvme/io/pram_block_writer.h"
#include "nvme/scheduler/message_hub.h"
#include "nvme/scheduler/scheduler.h"
#include "nvme/update_stores/nvme_update_store.h"

namespace sics::graph::nvme::apis {

using core::data_structures::Serializable;
using Block32 = data_structures::graph::BlockCSRGraphUInt32;

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
  BlockModel(const std::string& root_path)
      : scheduler_(root_path), update_store_(scheduler_.GetGraphMetadata()) {
    loader_ = std::make_unique<components::Loader<io::PramBlockReader>>(
        root_path, scheduler_.GetMessageHub());
    discharge_ = std::make_unique<components::Discharger<io::PramBlockWriter>>(
        root_path, scheduler_.GetMessageHub());
    executor_ =
        std::make_unique<components::Executor>(scheduler_.GetMessageHub());

    scheduler_.Init(&update_store_, executor_->GetTaskRunner(),
                    loader_->GetReader());
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

  void MapVertex(std::function<void(VertexID)>* func_vertex) {
    // all blocks should be executor the vertex function
    ExecuteMessage message;
    message.map_type = MapType::kMapVertex;
    message.func_vertex = func_vertex;
    scheduler_.RunMapExecute(message);
    LockAndWaitResult();
    LOG_INFO("MapVertex finished");
  }

  void MapEdge(std::function<void(VertexID, VertexID)>* func_edge) {
    // all blocks should be executor the edge function
    ExecuteMessage message;
    message.map_type = MapType::kMapEdge;
    message.func_edge = func_edge;
    scheduler_.RunMapExecute(message);
    LockAndWaitResult();
    LOG_INFO("MapEdge finished");
  }

  void MapAndMutateEdge(
      std::function<void(VertexID, VertexID, EdgeIndex)>* func_edge_del) {
    // all blocks should be executor the edge function
    ExecuteMessage message;
    message.map_type = MapType::kMapEdgeAndMutate;
    message.func_edge_mutate = func_edge_del;
    scheduler_.RunMapExecute(message);
    LockAndWaitResult();
    LOG_INFO("MapEdgeAndMutate finished");
  }

  void MapAndMutateEdgeBool(
      std::function<bool(VertexID, VertexID)>* func_edge_del) {
    ExecuteMessage message;
    message.map_type = MapType::kMapEdgeAndMutate;
    message.func_edge_mutate_bool = func_edge_del;
    scheduler_.RunMapExecute(message);
    LockAndWaitResult();
    LOG_INFO("MapEdgeAndMutate finished");
  }

  // ===============================================================
  // Map functions should use scheduler to iterate over all blocks
  // ===============================================================

  void Run() {
    LOG_INFO("Start running");
    begin_time_ = std::chrono::system_clock::now();
    loader_->Start();
    discharge_->Start();
    executor_->Start();
    scheduler_.Start();

    Compute();
  }

  void Stop() {
    scheduler_.Stop();
    loader_->StopAndJoin();
    discharge_->StopAndJoin();
    executor_->StopAndJoin();
  }

  void LockAndWaitResult() {
    //    std::unique_lock<std::mutex> lock(pram_mtx_);
    //    pram_cv_.wait(lock, [] { return true; });
    std::unique_lock<std::mutex> lock(*scheduler_.GetPramMtx());
    auto pram_ready = scheduler_.GetPramReady();
    *pram_ready = false;
    scheduler_.GetPramCv()->wait(lock, [pram_ready] { return *pram_ready; });
  }

  VertexData Read(VertexID id) { return update_store_.Read(id); }

  void Write(VertexID id, VertexData vdata) { update_store_.Write(id, vdata); }

  void WriteMin(VertexID id, VertexData vdata) {
    update_store_.WriteMin(id, vdata);
  }

  void DeleteEdge(VertexID src, VertexID dst, EdgeIndex idx) {
    update_store_.DeleteEdge(idx);
  }

  // methods for graph
  size_t GetGraphEdges() const { return scheduler_.GetGraphEdges(); }

 protected:
  core::common::TaskRunner* exe_runner_ = nullptr;

  // all blocks are stored in the GraphState of scheduler
  scheduler::PramScheduler scheduler_;
  std::unique_ptr<components::Loader<io::PramBlockReader>> loader_;
  std::unique_ptr<components::Discharger<io::PramBlockWriter>> discharge_;
  std::unique_ptr<components::Executor> executor_;
  update_stores::PramUpdateStoreUInt32 update_store_;

  int round_ = 0;

  //  std::mutex pram_mtx_;
  //  std::condition_variable pram_cv_;

  std::chrono::time_point<std::chrono::system_clock> begin_time_;
  std::chrono::time_point<std::chrono::system_clock> end_time_;

  // configs
  const uint32_t parallelism_ = 10;
  const uint32_t task_package_factor_ = 10;
};

}  // namespace sics::graph::nvme::apis

#endif  // GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_
