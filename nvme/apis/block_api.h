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
#include "nvme/common/config.h"
#include "nvme/components/discharge.h"
#include "nvme/components/executor.h"
#include "nvme/components/loader.h"
#include "nvme/data_structures/graph/pram_block.h"
#include "nvme/data_structures/neighbor_hop.h"
#include "nvme/io/pram_block_reader.h"
#include "nvme/io/pram_block_writer.h"
#include "nvme/scheduler/message_hub.h"
#include "nvme/scheduler/scheduler.h"
#include "nvme/update_stores/nvme_update_store.h"

namespace sics::graph::nvme::apis {

using core::data_structures::Serializable;
using Block32 = data_structures::graph::BlockCSRGraphUInt32;

template <typename TV>
class BlockModel : public BlockModelBase {
  using VertexData = TV;
  using EdgeData = core::common::DefaultEdgeDataType;

  using GraphID = core::common::GraphID;
  using VertexID = core::common::VertexID;
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexDegree = core::common::VertexDegree;

  using FuncVertex = core::common::FuncVertex;
  using FuncEdge = core::common::FuncEdge;
  using FuncEdgeAndMutate = core::common::FuncEdgeAndMutate;
  using FuncEdgeMutate = core::common::FuncEdgeMutate;

  using ExecuteMessage = sics::graph::nvme::scheduler::ExecuteMessage;
  using ExecuteType = sics::graph::nvme::scheduler::ExecuteType;
  using MapType = sics::graph::nvme::scheduler::MapType;

 public:
  BlockModel() = default;
  BlockModel(const std::string& root_path)
      : root_path_(root_path),
        scheduler_(root_path),
        update_store_(scheduler_.GetGraphMetadata()) {
    task_package_factor_ =
        core::common::Configurations::Get()->task_package_factor;
    loader_ = std::make_unique<components::Loader<io::PramBlockReader>>(
        root_path, scheduler_.GetMessageHub());
    discharge_ = std::make_unique<components::Discharger<io::PramBlockWriter>>(
        root_path, scheduler_.GetMessageHub());
    executor_ = std::make_unique<components::Executor<VertexData, EdgeData>>(
        scheduler_.GetMessageHub());

    scheduler_.Init(&update_store_, executor_->GetTaskRunner(),
                    loader_->GetReader());
  }

  ~BlockModel() override {
    Stop();
    whole_end_time_ = std::chrono::system_clock::now();
    if (core::common::Configurations::Get()->in_memory) {
      LOGF_INFO(" =========== In memory time: {} s ===========",
                std::chrono::duration<double>(common::end_time_in_memory -
                                              common::start_time_in_memory)
                    .count());
    }
    LOGF_INFO(
        " =========== Compute Runtime: {} s ===========",
        std::chrono::duration<double>(compute_end_time_ - begin_time_).count());
    LOGF_INFO(
        " =========== Whole Runtime: {} s ===========",
        std::chrono::duration<double>(whole_end_time_ - begin_time_).count());
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

  void MapVertexWithPrecomputing(FuncVertex* func_vertex) {
    ParallelVertexDo(*func_vertex);
    LOG_INFO("MapVertexWithPrecomputing finishes");
  }

  void MapEdge(std::function<void(VertexID, VertexID)>* func_edge) {
    // all blocks should be executor the edge function
    ExecuteMessage message;
    message.map_type = MapType::kMapEdge;
    message.func_edge = func_edge;
    scheduler_.RunMapExecute(message);
    LockAndWaitResult();
    LOG_INFO("MapEdge finishes");
  }

  void MapAndMutateEdge(
      std::function<void(VertexID, VertexID, EdgeIndex)>* func_edge_del) {
    // all blocks should be executor the edge function
    ExecuteMessage message;
    message.map_type = MapType::kMapEdgeAndMutate;
    message.func_edge_mutate = func_edge_del;
    scheduler_.RunMapExecute(message);
    LockAndWaitResult();
    LOG_INFO("MapEdgeAndMutate finishes");
  }

  void MapAndMutateEdgeBool(
      std::function<bool(VertexID, VertexID)>* func_edge_del) {
    ExecuteMessage message;
    message.map_type = MapType::kMapEdgeAndMutate;
    message.func_edge_mutate_bool = func_edge_del;
    scheduler_.RunMapExecute(message);
    LockAndWaitResult();
    LOG_INFO("MapEdgeAndMutate finishes");
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

    LOG_INFO(" ================ Start Algorithm executing! ================= ");
    if (core::common::Configurations::Get()->use_two_hop) {
      neighbor_hop_info_.Init(root_path_, scheduler_.GetGraphMetadata());
    }
    Compute();
    common::end_time_in_memory = std::chrono::system_clock::now();
    compute_end_time_ = std::chrono::system_clock::now();
    if (scheduler_.ReleaseResources()) {
      LockAndWaitResult();
    }
    LOG_INFO("Running finished!");
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

  // Used for block api.

  VertexData Read(VertexID id) { return update_store_.Read(id); }

  VertexData* WritePtr(VertexID id) { return update_store_.WritePtr(id); }

  void Write(VertexID id, VertexData vdata) { update_store_.Write(id, vdata); }

  void WriteMin(VertexID id, VertexData vdata) {
    update_store_.WriteMin(id, vdata);
  }

  void WriteAdd(VertexID id, VertexData vdata) {
    update_store_.WriteAdd(id, vdata);
  }

  void WriteAddDirect(VertexID id, VertexData vdata) {
    update_store_.WriteAddDirect(id, vdata);
  }

  void DeleteEdge(VertexID src, VertexID dst, EdgeIndex idx) {
    update_store_.DeleteEdge(idx);
  }

  VertexDegree GetOutDegree(VertexID id) { return scheduler_.GetOutDegree(id); }

  const VertexID* GetEdges(VertexID id) { return scheduler_.GetEdges(id); }

  VertexID GetNumVertices() { return scheduler_.GetVertexNumber(); }

  VertexID GetMinOneHop(VertexID id) {
    return neighbor_hop_info_.GetMinOneHop(id);
  }
  VertexID GetMaxOneHop(VertexID id) {
    return neighbor_hop_info_.GetMaxOneHop(id);
  }
  VertexID GetMinTwoHop(VertexID id) {
    return neighbor_hop_info_.GetMinTwoHop(id);
  }
  VertexID GetMaxTwoHop(VertexID id) {
    return neighbor_hop_info_.GetMaxTwoHop(id);
  }

  void ParallelVertexDo(FuncVertex& vertex_func) {
    auto num_vertices = scheduler_.GetVertexNumber();
    auto task_num = parallelism_ * task_package_factor_;
    uint32_t task_size = (num_vertices + task_num - 1) / task_num;
    core::common::TaskPackage tasks;
    //    tasks.reserve(ceil((double)block->GetVertexNums() / task_size));
    VertexID begin_id = 0, end_id = 0;
    for (; begin_id < num_vertices;) {
      end_id += task_size;
      if (end_id > num_vertices) {
        end_id = num_vertices;
      }
      auto task = [&vertex_func, begin_id, end_id]() {
        for (VertexID id = begin_id; id < end_id; id++) {
          vertex_func(id);
        }
      };
      tasks.push_back(task);
      begin_id = end_id;
    }
    LOGF_INFO("Precomputing task num: {}", tasks.size());
    executor_->GetTaskRunner()->SubmitSync(tasks);
  }

  // methods for graph
  size_t GetGraphEdges() const { return scheduler_.GetGraphEdges(); }

  // two hop info
  VertexDegree GetTwoHopOutDegree(VertexID id) { return 0; }

 protected:
  core::common::TaskRunner* exe_runner_ = nullptr;

  // all blocks are stored in the GraphState of scheduler
  scheduler::PramScheduler<VertexData> scheduler_;
  std::unique_ptr<components::Loader<io::PramBlockReader>> loader_;
  std::unique_ptr<components::Discharger<io::PramBlockWriter>> discharge_;
  std::unique_ptr<components::Executor<VertexData, EdgeData>> executor_;
  update_stores::PramNvmeUpdateStore<VertexData, EdgeData> update_store_;

  data_structures::NeighborHopInfo neighbor_hop_info_;

  int round_ = 0;

  //  std::mutex pram_mtx_;
  //  std::condition_variable pram_cv_;

  std::chrono::time_point<std::chrono::system_clock> begin_time_;
  std::chrono::time_point<std::chrono::system_clock> compute_end_time_;
  std::chrono::time_point<std::chrono::system_clock> whole_end_time_;

  std::string root_path_ = "";

  // configs
  uint32_t parallelism_ = 10;
  uint32_t task_package_factor_ = 50;
};

}  // namespace sics::graph::nvme::apis

#endif  // GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_
