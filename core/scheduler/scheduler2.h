#ifndef GRAPH_SYSTEMS_SCHEDULER2_H
#define GRAPH_SYSTEMS_SCHEDULER2_H

#include <cstdlib>
#include <random>

#include "apis/pie.h"
#include "apis/planar_app_base.h"
#include "apis/planar_app_group_base.h"
#include "common/config.h"
#include "common/multithreading/thread_pool.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"
#include "data_structures/graph/mutable_group_csr_grah.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serializable.h"
#include "scheduler/edge_buffer2.h"
#include "scheduler/graph_state.h"
#include "scheduler/message_hub.h"
#include "update_stores/update_store_base.h"

namespace sics::graph::core::scheduler {

class Scheduler2 {
  using MutableCSRGraphUInt32 = data_structures::graph::MutableCSRGraphUInt32;
  using MutableCSRGraphUInt16 = data_structures::graph::MutableCSRGraphUInt16;
  using MutableCSRGraphFloat = data_structures::graph::MutableCSRGraphFloat;
  using MutableGroupCSRGraphUInt32 =
      data_structures::graph::MutableGroupCSRGraphUInt32;
  using MutableGroupCSRGraphUInt16 =
      data_structures::graph::MutableGroupCSRGraphUInt16;
  using GraphID = common::GraphID;

 public:
  Scheduler2(const std::string& root_path)
      : graph_metadata_info_(root_path),
        current_round_(0),
        graph_state_(graph_metadata_info_.get_num_subgraphs()) {
    is_block_mode_ = graph_metadata_info_.get_type() == "block";
    memory_left_size_ = common::Configurations::Get()->memory_size;
    limits_ = common::Configurations::Get()->limits;
    use_limits_ = limits_ != 0;
    LOGF_INFO(
        "Scheduler create! Use limits for graph pre-fetch, can pre-fetch {}",
        limits_);
    short_cut_ = common::Configurations::Get()->short_cut;
    // 3/4 mode
    threefour_mode_ = common::Configurations::Get()->threefour_mode;
    // group mode
    group_mode_ = common::Configurations::Get()->group;
    group_graphs_.reserve(graph_metadata_info_.get_num_subgraphs());
    group_num_ = common::Configurations::Get()->group_num;
    srand(0);
  }

  virtual ~Scheduler2() = default;

  void Init(common::TaskRunner* task_runner, apis::PIE* app,
            data_structures::TwoDMetadata* meta,
            std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
            EdgeBuffer2* buffer) {
    metadata_ = meta;
    executor_task_runner_ = task_runner;
    app_ = app;
    graphs_ = graphs;
    buffer_ = buffer;
    mode_ = common::Configurations::Get()->mode;
  }

  int GetCurrentRound() const { return current_round_; }

  // schedule subgraph execute and its IO(read and write)
  void Start();

  void Stop() { thread_->join(); }

  MessageHub* GetMessageHub() { return &message_hub_; }

  size_t GetVertexNumber() const {
    return graph_metadata_info_.get_num_vertices();
  }

  void SetStatePtr(GraphState* state) { static_state_ = state; }

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

  common::GraphID GetNextReadGraphInCurrentRound() const { return 0; }

  common::GraphID GetNextExecuteGraph() const;

  common::GraphID GetNextReadGraphInNextRound() const;

  void GetNextExecuteGroupGraphs();

  size_t GetLeftPendingGraphNums() const;

  bool IsCurrentRoundFinish() const;

  // If current and next round both have no graph to read, system stop.
  bool IsSystemStop() const;

  void ReleaseAllGraph(common::GraphID gid) { buffer_->ReleaseBuffer(gid); }

  void SetAppRuntimeGraph(common::GraphID gid);

  void SetAppRound(int round);

  scheduler::ExecuteType GetExecuteType(GraphID gid) {
    if (mode_ != common::Normal) {
      auto round = static_state_->GetSubgraphRound(gid);
      return round == 0 ? scheduler::ExecuteType::kPEval
                        : scheduler::ExecuteType::kIncEval;
    } else {
      auto round = graph_state_.GetSubgraphRound(gid);
      return round == 0 ? scheduler::ExecuteType::kPEval
                        : scheduler::ExecuteType::kIncEval;
    }
  }

  void ReleaseAllSubEdgeBlocks() {
    for (int i = 0; i < metadata_->num_blocks; i++) {
      graphs_->at(i).ReleaseAllSubBlocks();
    }
  }

 private:
  // graph metadata: graph info, dependency matrix, subgraph metadata, etc.
  data_structures::GraphMetadata graph_metadata_info_;
  data_structures::TwoDMetadata* metadata_;
  bool is_block_mode_ = false;
  int current_round_ = 0;

  GraphState graph_state_;
  GraphState* static_state_;

  // message hub
  MessageHub message_hub_;

  // ExecuteMessage info, used for setting APP context
  update_stores::UpdateStoreBase* update_store_;
  common::TaskRunner* executor_task_runner_;
  apis::PIE* app_;

  // mark if the executor is running
  bool is_executor_running_ = false;
  std::unique_ptr<std::thread> thread_;

  size_t memory_left_size_ = 0;
  int limits_ = 0;
  bool use_limits_ = false;
  bool short_cut_ = true;
  bool threefour_mode_ = false;

  // group mode
  bool group_mode_ = false;
  size_t group_num_ = 0;
  size_t group_serialized_num_ = 0;
  size_t group_deserialized_num_ = 0;
  std::vector<common::GraphID> group_graphs_;
  std::unique_ptr<data_structures::Serializable> group_serializable_graph_;

  std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs_;
  EdgeBuffer2* buffer_;

  size_t to_read_graphs_ = 0;
  size_t have_read_graphs_ = 0;
  size_t need_read_graphs_ = 0;

  bool memory_enough_ = false;

  // Buffer managements.

  common::ModeType mode_ = common::Normal;

  int test = 0;
};

}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_SCHEDULER2_H
