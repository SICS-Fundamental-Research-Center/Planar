#ifndef GRAPH_SYSTEMS_NVME_SCHEDULER_SCHEDULER_H_
#define GRAPH_SYSTEMS_NVME_SCHEDULER_SCHEDULER_H_

#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <random>

#include "core/common/config.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/common/types.h"
#include "core/data_structures/graph_metadata.h"
#include "core/data_structures/serializable.h"
#include "core/data_structures/serialized.h"
#include "nvme/data_structures/graph/pram_block.h"
#include "nvme/io/pram_block_reader.h"
#include "nvme/scheduler/graph_state.h"
#include "nvme/scheduler/message_hub.h"
#include "nvme/update_stores/nvme_update_store.h"

namespace sics::graph::nvme::scheduler {

class PramScheduler {
  using MessageHub = sics::graph::nvme::scheduler::MessageHub;
  using Message = sics::graph::nvme::scheduler::Message;
  using ReadMessage = sics::graph::nvme::scheduler::ReadMessage;
  using WriteMessage = sics::graph::nvme::scheduler::WriteMessage;
  using ExecuteMessage = sics::graph::nvme::scheduler::ExecuteMessage;
  using ExecuteType = sics::graph::nvme::scheduler::ExecuteType;
  using MapType = sics::graph::nvme::scheduler::MapType;
  using GraphState = sics::graph::nvme::scheduler::GraphState;

  using BlockCSRGraphUInt32 = data_structures::graph::BlockCSRGraphUInt32;
  using BlockCSRGraphUInt16 = data_structures::graph::BlockCSRGraphUInt16;

  using GraphID = core::common::GraphID;
  using EdgeIndex = core::common::EdgeIndex;

 public:
  PramScheduler(const std::string& root_path)
      : graph_metadata_info_(root_path),
        graph_state_(graph_metadata_info_.get_num_subgraphs()) {
    is_block_mode_ = core::common::Configurations::Get()->is_block_mode;
    memory_left_size_ = core::common::Configurations::Get()->memory_size;
    limits_ = core::common::Configurations::Get()->limits;
    use_limits_ = limits_ != 0;
    if (use_limits_) {
      LOGF_INFO(
          "Scheduler create! Use limits for graph pre-fetch, can pre-fetch {}",
          limits_);
    } else {
      LOGF_INFO(
          "Scheduler create! Use memory buffer for graph pre-fetch. buffer "
          "size: {} MB",
          memory_left_size_);
    }
    short_cut_ = core::common::Configurations::Get()->short_cut;
    // group mode
    group_mode_ = core::common::Configurations::Get()->group;
    group_graphs_.reserve(graph_metadata_info_.get_num_subgraphs());
    group_num_ = core::common::Configurations::Get()->group_num;
    in_memory_ = core::common::Configurations::Get()->in_memory;
    srand(0);
  }

  virtual ~PramScheduler() = default;

  void Init(update_stores::PramUpdateStoreUInt32* update_store,
            core::common::TaskRunner* task_runner,
            io::PramBlockReader* reader) {
    update_store_ = update_store;
    executor_task_runner_ = task_runner;
    //    app_ = app;
    loader_ = reader;
  }

  // schedule subgraph execute and its IO(read and write)
  void Start();

  // TODO: decide which type message to send
  void Stop() {
    scheduler::ExecuteMessage execute_msg;
    execute_msg.terminated = true;
    message_hub_.SendResponse(scheduler::Message(execute_msg));
    thread_->join();
    LOG_INFO("*** Scheduler stop! ***");
  }

  bool ReleaseResources() {
    release_ = true;
    return ReleaseInMemoryGraph();
  }

  MessageHub* GetMessageHub() { return &message_hub_; }

  size_t GetVertexNumber() const {
    return graph_metadata_info_.get_num_vertices();
  }

  EdgeIndex GetEdgeNumber() const {
    return graph_metadata_info_.get_num_edges();
  }

  void RunMapExecute(ExecuteMessage execute_msg);

  size_t GetGraphEdges() const { graph_metadata_info_.get_num_edges(); }

  const core::data_structures::GraphMetadata& GetGraphMetadata() {
    return graph_metadata_info_;
  }

  // mtx and cv
  std::mutex* GetPramMtx() { return &pram_mtx_; }
  std::condition_variable* GetPramCv() { return &pram_cv_; }
  bool* GetPramReady() { return &pram_ready_; }

  GraphID GetCurrentBlockId() const { return current_bid_; }

 protected:
  virtual bool ReadMessageResponseAndExecute(const ReadMessage& read_resp);

  virtual bool ExecuteMessageResponseAndWrite(
      const ExecuteMessage& execute_resp);

  virtual bool WriteMessageResponseAndCheckTerminate(
      const WriteMessage& write_resp);

 private:
  // TODO: check memory size to decide if read next graph.
  // try to read next graph will be executed.
  // in current round or next round
  bool TryReadNextGraph(bool sync = false);

  core::data_structures::Serializable* CreateSerializableGraph(
      core::common::GraphID graph_id);
  core::data_structures::Serialized* CreateSerialized(
      core::common::GraphID graph_id);

  GraphID GetNextReadGraphInCurrentRound() const;

  GraphID GetNextExecuteGraph() const;

  GraphID GetNextReadGraphInNextRound() const;

  GraphID GetNextExecuteGraphInMemory() const;

  void GetNextExecuteGroupGraphs();

  size_t GetLeftPendingGraphNums() const;

  bool IsCurrentRoundFinish() const;

  // If current and next round both have no graph to read, system stop.
  bool IsSchedulerStop() const;

  void ReleaseAllGraph();

  void SetAppRuntimeGraph(GraphID gid);

  void SetGlobalVertexData(GraphID bid);

  void SetExecuteMessageMapFunction(ExecuteMessage* message);

  void ResetMapFunction();

  void UnlockAndReleaseResult();

  void CheckMapFunctionFinish();

  bool ReleaseInMemoryGraph();

  void SendExecuteMessage(GraphID bid);

  void ExecuteNextGraphInMemory();

  void SendWriteMessage(GraphID bid);

 private:
  // graph metadata: graph info, dependency matrix, subgraph metadata, etc.
  core::data_structures::GraphMetadata graph_metadata_info_;
  bool is_block_mode_ = false;
  GraphState graph_state_;

  // message hub
  MessageHub message_hub_;

  // ExecuteMessage info, used for setting APP context
  update_stores::PramUpdateStoreUInt32* update_store_;
  core::common::TaskRunner* executor_task_runner_;

  std::mutex pram_mtx_;
  std::condition_variable pram_cv_;
  bool pram_ready_ = false;

  // Loader
  io::PramBlockReader* loader_ = nullptr;

  // mark if the executor is running
  bool is_executor_running_ = false;
  bool release_ = false;

  std::unique_ptr<std::thread> thread_;

  MapType current_Map_type_ = MapType::kDefault;
  core::common::FuncVertex* func_vertex_ = nullptr;
  core::common::FuncEdge* func_edge_ = nullptr;
  core::common::FuncEdgeMutate* func_edge_mutate_bool_ = nullptr;
  core::common::GraphID current_bid_ = 0;

  size_t step_ = 0;

  size_t memory_left_size_;
  int limits_ = 0;
  bool use_limits_ = false;
  bool short_cut_ = true;
  bool in_memory_ = false;

  // group mode
  bool group_mode_ = false;
  int group_num_ = 0;
  int group_serialized_num_ = 0;
  int group_deserialized_num_ = 0;
  std::vector<GraphID> group_graphs_;
  std::unique_ptr<core::data_structures::Serializable>
      group_serializable_graph_;

  int to_read_graphs_ = 0;
  int have_read_graphs_ = 0;
  int need_read_graphs_ = 0;

  int test = 0;
};

}  // namespace sics::graph::nvme::scheduler

#endif  // GRAPH_SYSTEMS_NVME_SCHEDULER_SCHEDULER_H_
