#ifndef GRAPH_SYSTEMS_SCHEDULER_H
#define GRAPH_SYSTEMS_SCHEDULER_H

#include "data_structures/graph_metadata.h"
#include "update_stores/update_store_base.h"
#include "scheduler/message_hub.h"
#include "scheduler/subgraph_state.h"

namespace sics::graph::core::scheduler {

class Scheduler {
 public:
  Scheduler() = default;
  virtual ~Scheduler() = default;

  int GetCurrentRound() const { return current_round_; }

  // read graph metadata from meta.yaml file
  // meta file path should be passed by gflags
  void ReadAndParseGraphMetadata(const std::string& graph_metadata_path);

  // global message store

  // schedule subgraph execute and its IO(read and write)
  void Start();

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

  common::GraphID GetNextReadGraphInCurrentRound() const;

  // get next execute graph in current round
  common::GraphID GetNextExecuteGraph() const;

  common::GraphID GetNextReadGraphInNextRound() const;

  bool IsCurrentRoundFinish() {
    return GetNextReadGraphInCurrentRound() == INVALID_GRAPH_ID;
  }

  bool IsSystemStop() {
    // if current and next round both have no graph to read, system stop
    return (GetNextReadGraphInCurrentRound() == INVALID_GRAPH_ID) &&
           (GetNextReadGraphInNextRound() == INVALID_GRAPH_ID);
  }

 private:
  // graph metadata: graph info, dependency matrix, subgraph metadata, etc.
  data_structures::GraphMetadata graph_metadata_info_;
  GraphState graph_state_;

  int current_round_ = 0;

  // message hub
  MessageHub message_hub_;

  // only keep a reference to global message store
  update_stores::UpdateStoreBase* global_update_store_;

  std::unique_ptr<std::thread> thread_;
};

}  // namespace sics::graph::core::scheduler

#endif  // GRAPH_SYSTEMS_SCHEDULER_H
