#ifndef GRAPH_SYSTEMS_NVME_SCHEDULER_SCHEDULER_H_
#define GRAPH_SYSTEMS_NVME_SCHEDULER_SCHEDULER_H_

#include <cstdlib>
#include <random>

#include "core/apis/pie.h"
#include "core/apis/planar_app_base.h"
#include "core/apis/planar_app_group_base.h"
#include "core/common/config.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/common/types.h"
#include "core/data_structures/graph/mutable_csr_graph.h"
#include "core/data_structures/graph/mutable_group_csr_grah.h"
#include "core/data_structures/graph/pram_block.h"
#include "core/data_structures/graph_metadata.h"
#include "core/data_structures/serializable.h"
#include "core/io/mutable_csr_reader.h"
#include "core/scheduler/graph_state.h"
#include "core/scheduler/message_hub.h"
#include "core/update_stores/update_store_base.h"

namespace sics::graph::nvme::scheduler {

using namespace sics::graph::core;

class PramScheduler {
  using MessageHub = sics::graph::core::scheduler::MessageHub;
  using Message = sics::graph::core::scheduler::Message;
  using ReadMessage = sics::graph::core::scheduler::ReadMessage;
  using WriteMessage = sics::graph::core::scheduler::WriteMessage;
  using ExecuteMessage = sics::graph::core::scheduler::ExecuteMessage;
  using GraphState = sics::graph::core::scheduler::GraphState;
  using ExecuteType = sics::graph::core::scheduler::ExecuteType;

  using MutableCSRGraphUInt32 = data_structures::graph::MutableCSRGraphUInt32;
  using MutableCSRGraphUInt16 = data_structures::graph::MutableCSRGraphUInt16;
  using MutableGroupCSRGraphUInt32 =
      data_structures::graph::MutableGroupCSRGraphUInt32;
  using MutableGroupCSRGraphUInt16 =
      data_structures::graph::MutableGroupCSRGraphUInt16;
  using BlockCSRGraphUInt32 = data_structures::graph::BlockCSRGraphUInt32;
  using BlockCSRGraphUInt16 = data_structures::graph::BlockCSRGraphUInt16;

 public:
  PramScheduler(const std::string& root_path)
      : graph_metadata_info_(root_path),
        current_round_(0),
        graph_state_(graph_metadata_info_.get_num_subgraphs()) {
    is_block_mode_ = common::Configurations::Get()->is_block_mode;
    memory_left_size_ = common::Configurations::Get()->memory_size;
    limits_ = common::Configurations::Get()->limits;
    use_limits_ = limits_ != 0;
    LOGF_INFO(
        "Scheduler create! Use limits for graph pre-fetch, can pre-fetch {}",
        limits_);
    short_cut_ = common::Configurations::Get()->short_cut;
    // group mode
    group_mode_ = common::Configurations::Get()->group;
    group_graphs_.reserve(graph_metadata_info_.get_num_subgraphs());
    group_num_ = common::Configurations::Get()->group_num;
    srand(0);
  }

  virtual ~PramScheduler() = default;

  void Init(update_stores::UpdateStoreBase* update_store,
            common::TaskRunner* task_runner, apis::PIE* app,
            io::MutableCSRReader* loader) {
    update_store_ = update_store;
    executor_task_runner_ = task_runner;
    app_ = app;
    loader_ = loader;
  }

  int GetCurrentRound() const { return current_round_; }

  // schedule subgraph execute and its IO(read and write)
  void Start();

  void Stop() { thread_->join(); }

  MessageHub* GetMessageHub() { return &message_hub_; }

  size_t GetVertexNumber() const {
    return graph_metadata_info_.get_num_vertices();
  }

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

  void CreateSerializableGraph(common::GraphID graph_id);
  data_structures::Serialized* CreateSerialized(common::GraphID graph_id);
  void CreateGroupSerializableGraph();

  void InitGroupSerializableGraph();

  common::GraphID GetNextReadGraphInCurrentRound() const;

  common::GraphID GetNextExecuteGraph() const;

  common::GraphID GetNextReadGraphInNextRound() const;

  void GetNextExecuteGroupGraphs();

  size_t GetLeftPendingGraphNums() const;

  bool IsCurrentRoundFinish() const;

  // If current and next round both have no graph to read, system stop.
  bool IsSystemStop() const;

  void ReleaseAllGraph();

  void SetAppRuntimeGraph(common::GraphID gid);

  void SetAppRound(int round);

 private:
  // graph metadata: graph info, dependency matrix, subgraph metadata, etc.
  data_structures::GraphMetadata graph_metadata_info_;
  bool is_block_mode_ = false;
  GraphState graph_state_;

  int current_round_ = 0;

  // message hub
  MessageHub message_hub_;

  // ExecuteMessage info, used for setting APP context
  update_stores::UpdateStoreBase* update_store_;
  common::TaskRunner* executor_task_runner_;
  apis::PIE* app_;

  // Loader
  io::MutableCSRReader* loader_ = nullptr;

  // mark if the executor is running
  bool is_executor_running_ = false;
  std::unique_ptr<std::thread> thread_;

  size_t memory_left_size_ = 0;
  int limits_ = 0;
  bool use_limits_ = false;
  bool short_cut_ = true;

  // group mode
  bool group_mode_ = false;
  int group_num_ = 0;
  int group_serialized_num_ = 0;
  int group_deserialized_num_ = 0;
  std::vector<common::GraphID> group_graphs_;
  std::unique_ptr<data_structures::Serializable> group_serializable_graph_;

  int to_read_graphs_ = 0;
  int have_read_graphs_ = 0;
  int need_read_graphs_ = 0;

  int test = 0;
};

}  // namespace sics::graph::nvme::scheduler

#endif  // GRAPH_SYSTEMS_NVME_SCHEDULER_SCHEDULER_H_
