#ifndef GRAPH_SYSTEMS_NVME_COMPONENTS_EXECUTOR_H_
#define GRAPH_SYSTEMS_NVME_COMPONENTS_EXECUTOR_H_

#include <chrono>
#include <memory>
#include <thread>
#include <type_traits>

#include "core/apis/pie.h"
#include "core/common/config.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/components/component.h"
#include "core/data_structures/graph/pram_block.h"
#include "core/util/logging.h"
#include "nvme/components/component.h"
#include "nvme/scheduler/message_hub.h"

namespace sics::graph::nvme::components {

class Executor : public Component {
  using GraphID = core::common::GraphID;
  using VertexID = core::common::VertexID;
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using Block32 = core::data_structures::graph::BlockCSRGraphUInt32;
  using Block16 = core::data_structures::graph::BlockCSRGraphUInt16;

 public:
  Executor(scheduler::MessageHub* hub)
      : execute_q_(hub->get_executor_queue()),
        response_q_(hub->get_response_queue()),
        task_runner_(core::common::Configurations::Get()->parallelism) {
    in_memory_time_ = core::common::Configurations::Get()->in_memory;
  }
  ~Executor() final = default;

  void Start() override {
    thread_ = std::make_unique<std::thread>([this]() {
      while (true) {
        scheduler::ExecuteMessage message = execute_q_->PopOrWait();
        if (message.terminated) {
          LOG_INFO("*** Executor is signaled termination ***");
          break;
        }

        LOGF_INFO("Executor starts executing block {}", message.graph_id);
        // TODO: execute api logic
        switch (message.execute_type) {
          case scheduler::ExecuteType::kDeserialize: {
            LOGF_INFO("Executor: Deserialize block {}", message.graph_id);
            core::data_structures::Serializable* graph = message.graph;
            graph->Deserialize(
                task_runner_,
                std::unique_ptr<core::data_structures::Serialized>(
                    message.serialized));
            message.response_serializable = graph;
            break;
          }
          case scheduler::ExecuteType::kCompute:
            LOGF_INFO("Executor: PEval block {}", message.graph_id);
            if (in_memory_time_) start_time_ = std::chrono::system_clock::now();
            message.app->PEval();
            if (in_memory_time_) end_time_ = std::chrono::system_clock::now();
            break;
          case scheduler::ExecuteType::kSerialize:
            LOGF_INFO("Executor: Serialized block {}", message.graph_id);
            // Set serialized graph to message for write back to disk.
            message.serialized =
                message.graph->Serialize(task_runner_).release();
            break;
        }
        LOGF_INFO("Executor completes executing block {}", message.graph_id);
        response_q_->Push(scheduler::Message(message));
      }
    });
  }

  void StopAndJoin() override {
    scheduler::ExecuteMessage message;
    message.terminated = true;
    execute_q_->Push(message);
    // first stop the task_runner, then stop the Executor thread
    thread_->join();
    if (in_memory_time_) {
      LOGF_INFO("========== In memory time: {} s ==========",
                std::chrono::duration<double>(end_time_ - start_time_).count());
    }
  }

  core::common::TaskRunner* GetTaskRunner() { return &task_runner_; }

  void ParallelVertexDO(
      Block32* block,
      const std::function<void(VertexID, VertexIndex)>& vertex_func) {
    LOG_DEBUG("ParallelVertexDo is begin");
    uint32_t task_size = GetTaskSize(block->GetVertexNums());
    core::common::TaskPackage tasks;
    tasks.reserve(parallelism_ * task_package_factor_);
    VertexIndex begin_index = 0, end_index = 0;
    for (; begin_index < block->GetVertexNums();) {
      end_index += task_size;
      if (end_index > block->GetVertexNums())
        end_index = block->GetVertexNums();
      auto task = [&vertex_func, this, begin_index, end_index]() {
        for (VertexIndex idx = begin_index; idx < end_index; idx++) {
          //          vertex_func(block->GetVertexIDByIndex(idx));
        }
      };
      tasks.push_back(task);
      begin_index = end_index;
    }
    //    LOGF_INFO("ParallelVertexDo task_size: {}, num tasks: {}", task_size,
    //              tasks.size());
    task_runner_.SubmitSync(tasks);
    // TODO: sync of update_store and graph_ vertex data
    //    graph->SyncVertexData();
    LOG_DEBUG("ParallelVertexDo is done");
  }

  void ParallelEdgeMutateDo(
      Block32* block,
      const std::function<void(VertexID, VertexID, EdgeIndex)>& edge_del_func) {
    LOG_DEBUG("ParallelEdgeDelDo is begin");
    //    uint32_t task_size = GetTaskSize(block->GetVertexNums());
    uint32_t task_size = block->GetVertexNums();
    core::common::TaskPackage tasks;
    VertexIndex begin_index = 0, end_index = 0;
    for (; begin_index < block->GetVertexNums();) {
      end_index += task_size;
      if (end_index > block->GetVertexNums())
        end_index = block->GetVertexNums();
      auto task = [&edge_del_func, this, begin_index, end_index]() {
        for (VertexIndex i = begin_index; i < end_index; i++) {
          //          auto degree = block->GetOutDegreeByIndex(i);
          //          if (degree != 0) {
          //            EdgeIndex outOffset_base =
          //            block->GetOutOffsetByIndex(i); VertexID* outEdges =
          //            block->GetOutEdgesByIndex(i); for (VertexIndex j = 0; j
          //            < degree; j++) {
          //              edge_del_func(block->GetVertexIDByIndex(i),
          //              outEdges[j],
          //                            outOffset_base + j);
          //            }
          //          }
        }
      };
      tasks.push_back(task);
      begin_index = end_index;
    }
    task_runner_.SubmitSync(tasks);
    block->MutateGraphEdge(&task_runner_);
    LOG_DEBUG("ParallelEdgedelDo is done");
  }

  size_t GetTaskSize(VertexID max_vid) const {
    auto task_num = parallelism_ * task_package_factor_;
    size_t task_size = ceil((double)max_vid / task_num);
    return task_size < 2 ? 2 : task_size;
  }

 private:
  scheduler::ExecutorQueue* execute_q_;
  scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;
  core::common::ThreadPool task_runner_;

  bool in_memory_time_ = false;
  std::chrono::time_point<std::chrono::system_clock> start_time_;
  std::chrono::time_point<std::chrono::system_clock> end_time_;

  // configs
  const uint32_t parallelism_ = 8;
  const uint32_t task_package_factor_ = 100000;
};

}  // namespace sics::graph::nvme::components

#endif  // GRAPH_SYSTEMS_NVME_COMPONENTS_EXECUTOR_H_
