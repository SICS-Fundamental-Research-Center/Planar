#ifndef GRAPH_SYSTEMS_SCHEDULER_H
#define GRAPH_SYSTEMS_SCHEDULER_H

#include "apis/pie.h"
#include "apis/planar_app_base.h"
#include "common/config.h"
#include "common/multithreading/thread_pool.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serializable.h"
#include "scheduler/graph_state.h"
#include "scheduler/message_hub.h"
#include "update_stores/update_store_base.h"

namespace sics::graph::core::scheduler {

class Scheduler {
  using MutableCSRGraphUInt32 = data_structures::graph::MutableCSRGraphUInt32;
  using MutableCSRGraphUInt16 = data_structures::graph::MutableCSRGraphUInt16;

 public:
  Scheduler(const std::string& root_path)
      : graph_metadata_info_(root_path),
        current_round_(0),
        graph_state_(graph_metadata_info_.get_num_subgraphs()) {
    memory_left_size_ = common::Configurations::Get()->memory_size;
  }

  virtual ~Scheduler() = default;

  void Init(update_stores::UpdateStoreBase* update_store,
            common::TaskRunner* task_runner, apis::PIE* app) {
    update_store_ = update_store;
    executor_task_runner_ = task_runner;
    app_ = app;
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

  std::unique_ptr<data_structures::Serializable> CreateSerializableGraph(
      common::GraphID graph_id);

  common::GraphID GetNextReadGraphInCurrentRound() const;

  common::GraphID GetNextExecuteGraph() const;

  common::GraphID GetNextReadGraphInNextRound() const;

  bool IsCurrentRoundFinish() const;

  // If current and next round both have no graph to read, system stop.
  bool IsSystemStop() const;

  void ReleaseAllGraph();

  void SetRuntimeGraph(common::GraphID gid);

 private:
  // graph metadata: graph info, dependency matrix, subgraph metadata, etc.
  data_structures::GraphMetadata graph_metadata_info_;
  GraphState graph_state_;

  int current_round_ = 0;

  // message hub
  MessageHub message_hub_;

  // ExecuteMessage info, used for setting APP context
  update_stores::UpdateStoreBase* update_store_;
  common::TaskRunner* executor_task_runner_;
  apis::PIE* app_;

  std::unique_ptr<std::thread> thread_;

  size_t memory_left_size_ = 0;
};

}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_SCHEDULER_H
