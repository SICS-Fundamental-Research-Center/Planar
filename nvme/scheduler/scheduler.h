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

template <typename TV>
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
  using BlockCSRGraphFloat = data_structures::graph::BlockCSRGraphFloat;

  using GraphID = core::common::GraphID;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexDegree = core::common::VertexDegree;

  using VertexData = TV;
  using EdgeData = core::common::DefaultEdgeDataType;

  using BlockCSRGraph = data_structures::graph::PramBlock<TV, EdgeData>;

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

    graphs_.resize(graph_metadata_info_.get_num_subgraphs());
  }

  virtual ~PramScheduler() = default;

  void Init(
      update_stores::PramNvmeUpdateStore<VertexData, EdgeData>* update_store,
      core::common::TaskRunner* task_runner, io::PramBlockReader* reader) {
    update_store_ = update_store;
    executor_task_runner_ = task_runner;
    //    app_ = app;
    loader_ = reader;
  }

  // schedule subgraph execute and its IO(read and write)
  void Start() {
    thread_ = std::make_unique<std::thread>([this]() {
      LOG_INFO("*** PramScheduler starts ***");
      bool running = true;
      while (running) {
        Message resp = message_hub_.GetResponse();

        if (resp.is_terminated()) {
          break;
        }
        switch (resp.get_type()) {
          case Message::Type::kRead: {
            ReadMessage read_response;
            resp.Get(&read_response);
            ReadMessageResponseAndExecute(read_response);
            break;
          }
          case Message::Type::kExecute: {
            ExecuteMessage execute_response;
            resp.Get(&execute_response);
            ExecuteMessageResponseAndWrite(execute_response);
            break;
          }
          case Message::Type::kWrite: {
            WriteMessage write_response;
            resp.Get(&write_response);
            WriteMessageResponseAndCheckTerminate(write_response);
            break;
          }
          default:
            break;
        }
      }
      // LOG_INFO("*** PramScheduler is signaled termination ***");
    });
  }

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

  VertexID GetVertexNumber() const {
    return graph_metadata_info_.get_num_vertices();
  }

  EdgeIndex GetEdgeNumber() const {
    return graph_metadata_info_.get_num_edges();
  }

  void RunMapExecute(ExecuteMessage execute_msg) {
    message_hub_.get_response_queue()->Push(scheduler::Message(execute_msg));
  }

  size_t GetGraphEdges() const { graph_metadata_info_.get_num_edges(); }

  const core::data_structures::GraphMetadata& GetGraphMetadata() {
    return graph_metadata_info_;
  }

  // mtx and cv
  std::mutex* GetPramMtx() { return &pram_mtx_; }
  std::condition_variable* GetPramCv() { return &pram_cv_; }
  bool* GetPramReady() { return &pram_ready_; }

  GraphID GetCurrentBlockId() const { return current_bid_; }

  VertexDegree GetOutDegree(VertexID id) {
    auto serializable = GetBlock(current_bid_);
    auto graph = static_cast<BlockCSRGraph*>(serializable);
    return graph->GetOutDegreeByID(id);
  }

  const VertexID* GetEdges(VertexID id) {
    auto serializable = GetBlock(current_bid_);
    auto graph = static_cast<BlockCSRGraph*>(serializable);
    return graph->GetOutEdgesByID(id);
  }

 protected:
  virtual bool ReadMessageResponseAndExecute(const ReadMessage& read_resp) {
    // Read finish, to execute the loaded graph.
    graph_state_.SetGraphState(read_resp.graph_id, GraphState::Deserialized);
    // If executor is not running, execute the blocks.
    if (!is_executor_running_) {
      SendExecuteMessage(read_resp.graph_id);
    }
    TryReadNextGraph();
    return true;
  }

  virtual bool ExecuteMessageResponseAndWrite(
      const ExecuteMessage& execute_resp) {
    is_executor_running_ = false;

    // Require map type of current graph.
    if (current_Map_type_ == kDefault) {
      current_Map_type_ = execute_resp.map_type;
      switch (current_Map_type_) {
        case MapType::kMapVertex:
          func_vertex_ = execute_resp.func_vertex;
          break;
        case MapType::kMapEdge:
          func_edge_ = execute_resp.func_edge;
          break;
        case MapType::kMapEdgeAndMutate:
          func_edge_mutate_bool_ = execute_resp.func_edge_mutate_bool;
          break;
        default:
          LOG_ERROR("Map type is not supported!");
          break;
      }
      // Read first block.
      if (in_memory_) {
        // If in-memory mode, first round read, than keep the block in memory.
        ExecuteNextGraphInMemory();
      } else {
        TryReadNextGraph();
      }
    } else {
      //    if (in_memory_) {
      // Still keep deserialized state.
      graph_state_.SetCurrentRoundPendingFinish(execute_resp.graph_id);
      // Check if edges of block mutated.
      if (current_Map_type_ == MapType::kMapEdgeAndMutate) {
        graph_metadata_info_.UpdateOutEdgeNumInBLockMode();
        graph_state_.SetBlockMutated(execute_resp.graph_id);
      }
      if (!in_memory_) {
        // TODO: if memory is enough, keep the block in memory.
        SendWriteMessage(execute_resp.graph_id);  // Write back to disk.
      }
      if (IsCurrentRoundFinish()) {
        graph_state_.ResetCurrentRoundPending();
        update_store_->Sync();
        step_++;
        ResetMapFunction();
        // Return current map function scheduling.
        UnlockAndReleaseResult();
      } else {
        ExecuteNextGraphInMemory();
      }
      //    } else {
      //      ExecuteNextGraphInMemory();
      //    }
    }
    return true;
  }

  virtual bool WriteMessageResponseAndCheckTerminate(
      const WriteMessage& write_resp) {
    // Update subgraph state.
    graph_state_.SetGraphState(write_resp.graph_id, GraphState::OnDisk);
    auto write_size = graph_metadata_info_.GetBlockSize(write_resp.graph_id);
    //  LOGF_INFO(
    //      "Release subgraph: {}, size: {}. *** Memory size now: {}, after: {}
    //      "
    //      "***",
    //      write_resp.graph_id, write_size, memory_left_size_,
    //      memory_left_size_ + write_size);
    if (use_limits_) {
      limits_++;
      //    LOGF_INFO("Write back one graph. now limits is {}", limits_);
    } else {
      memory_left_size_ += write_size;
      graph_metadata_info_.UpdateBlockSize(write_resp.graph_id);
    }
    // Read next subgraph if permitted.
    if (release_) {
      if (IsSchedulerStop()) UnlockAndReleaseResult();
    } else {
      if (current_Map_type_ != MapType::kDefault) {
        TryReadNextGraph();
      }
    }
    return true;
  }

 private:
  // TODO: check memory size to decide if read next graph.
  // try to read next graph will be executed.
  // in current round or next round
  bool TryReadNextGraph(bool sync = false) {
    auto load_graph_id = GetNextReadGraphInCurrentRound();
    if (load_graph_id == INVALID_GRAPH_ID) return false;

    // group_mode use the same logic
    if (use_limits_) {
      if (limits_ <= 0) return false;
      limits_--;
      // LOGF_INFO("Read on graph {}. now limits is {}", load_graph_id,
      // limits_);
    } else {
      auto read_size = graph_metadata_info_.GetBlockSize(load_graph_id);
      if (memory_left_size_ < read_size) return false;
      //    LOGF_INFO(
      //        "To read subgraph {}. *** Memory size now: {}, after read: {} "
      //        "***",
      //        load_graph_id, memory_left_size_, memory_left_size_ -
      //        read_size);
      memory_left_size_ -= read_size;
    }

    // condition satisfied
    ReadMessage read_message;
    read_message.graph_id = load_graph_id;
    read_message.num_vertices =
        graph_metadata_info_.GetBlockNumVertices(load_graph_id);
    //  read_message.serialized = CreateSerialized(load_graph_id);
    read_message.graph = CreateSerializableGraph(load_graph_id);
    read_message.changed = graph_state_.IsBlockMutated(load_graph_id);
    graph_state_.SetOnDiskToReading(load_graph_id);
    message_hub_.get_reader_queue()->Push(read_message);
    return true;
  }

  core::data_structures::Serializable* GetBlock(GraphID bid) {
    return graphs_.at(bid).get();
  }

  core::data_structures::Serializable* CreateSerializableGraph(
      core::common::GraphID graph_id) {
    graphs_.at(graph_id) = std::make_unique<BlockCSRGraph>(
        graph_metadata_info_.GetBlockMetadataPtr(graph_id));
    return graphs_.at(graph_id).get();
  }

  GraphID GetNextReadGraphInCurrentRound() const {
    for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
      if (graph_state_.current_round_pending_.at(gid) &&
          graph_state_.subgraph_storage_state_.at(gid) == GraphState::OnDisk) {
        return gid;
      }
    }
    return INVALID_GRAPH_ID;
  }

  GraphID GetNextExecuteGraph() const {
    for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
      if (graph_state_.current_round_pending_.at(gid) &&
          graph_state_.subgraph_storage_state_.at(gid) ==
              GraphState::StorageStateType::Serialized) {
        return gid;
      }
    }
    return INVALID_GRAPH_ID;
  }

  GraphID GetNextExecuteGraphInMemory() const {
    for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
      if (graph_state_.current_round_pending_.at(gid) &&
          graph_state_.subgraph_storage_state_.at(gid) ==
              GraphState::StorageStateType::Deserialized) {
        return gid;
      }
    }
    return INVALID_GRAPH_ID;
  }

  size_t GetLeftPendingGraphNums() const {
    size_t res = 0;
    for (int gid = 0; gid < graph_metadata_info_.get_num_subgraphs(); gid++) {
      if (graph_state_.current_round_pending_.at(gid)) {
        res++;
      }
    }
    return res;
  }

  bool IsCurrentRoundFinish() const {
    for (int i = 0; i < graph_metadata_info_.get_num_subgraphs(); i++) {
      if (graph_state_.current_round_pending_.at(i)) {
        return false;
      }
    }
    return true;
  }

  // If current and next round both have no graph to read, system stop.
  bool IsSchedulerStop() const {
    for (int i = 0; i < graph_metadata_info_.get_num_subgraphs(); i++) {
      if (graph_state_.subgraph_storage_state_.at(i) !=
          GraphState::StorageStateType::OnDisk) {
        return false;
      }
    }
    return true;
  }

  void SetExecuteMessageMapFunction(ExecuteMessage* message) {
    switch (current_Map_type_) {
      case MapType::kMapVertex: {
        message->map_type = MapType::kMapVertex;
        message->func_vertex = func_vertex_;
        break;
      }
      case MapType::kMapEdge: {
        message->map_type = MapType::kMapEdge;
        message->func_edge = func_edge_;
        break;
      }
      case MapType::kMapEdgeAndMutate: {
        message->map_type = MapType::kMapEdgeAndMutate;
        message->func_edge_mutate_bool = func_edge_mutate_bool_;
        break;
      }
      default:
        LOG_ERROR("Map type is not supported!");
        break;
    }
  }

  void ResetMapFunction() {
    current_Map_type_ = MapType::kDefault;
    func_vertex_ = nullptr;
    func_edge_ = nullptr;
    func_edge_mutate_bool_ = nullptr;
  }

  void UnlockAndReleaseResult() {
    std::lock_guard<std::mutex> lock(pram_mtx_);
    pram_ready_ = true;
    pram_cv_.notify_all();
  }

  void CheckMapFunctionFinish() {
    if (current_Map_type_ != MapType::kDefault)
      LOGF_FATAL("Map type error when return from scheduler!",
                 current_Map_type_);
    UnlockAndReleaseResult();
  }

  void SendWriteMessage(GraphID bid) {
    WriteMessage write_message;
    write_message.graph_id = bid;
    write_message.graph = GetBlock(bid);
    write_message.changed = graph_state_.IsBlockMutated(bid);
    graph_state_.SetGraphState(bid, GraphState::StorageStateType::Writing);
    message_hub_.get_writer_queue()->Push(write_message);
  }

  bool ReleaseInMemoryGraph() {
    bool flag = false;
    for (int i = 0; i < graph_metadata_info_.get_num_subgraphs(); i++) {
      if (graph_state_.GetSubgraphState(i) == GraphState::Deserialized) {
        LOGF_INFO("Release block: {} from memory", i);
        SendWriteMessage(i);
        flag = true;
      }
    }
    return flag;
  }

  void SendExecuteMessage(GraphID bid) {
    ExecuteMessage execute_message;
    execute_message.graph_id = bid;
    execute_message.graph = GetBlock(bid);
    execute_message.execute_type = ExecuteType::kCompute;
    execute_message.map_type = current_Map_type_;
    SetExecuteMessageMapFunction(&execute_message);
    current_bid_ = bid;
    is_executor_running_ = true;
    message_hub_.get_executor_queue()->Push(execute_message);
  }

  void ExecuteNextGraphInMemory() {
    auto next_execute_gid = GetNextExecuteGraphInMemory();
    if (next_execute_gid == INVALID_GRAPH_ID) {
      TryReadNextGraph();
      return;
    }
    SendExecuteMessage(next_execute_gid);
  }

 private:
  // graph metadata: graph info, dependency matrix, subgraph metadata, etc.
  core::data_structures::GraphMetadata graph_metadata_info_;
  bool is_block_mode_ = false;
  GraphState graph_state_;

  // message hub
  MessageHub message_hub_;

  // ExecuteMessage info, used for setting APP context
  update_stores::PramNvmeUpdateStore<VertexData, EdgeData>* update_store_;
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

  // blocks
  std::vector<std::unique_ptr<core::data_structures::Serializable>> graphs_;
};

}  // namespace sics::graph::nvme::scheduler

#endif  // GRAPH_SYSTEMS_NVME_SCHEDULER_SCHEDULER_H_
