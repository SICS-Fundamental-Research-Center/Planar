#ifndef GRAPH_SYSTEMS_CORE_COMPONENTS_EXECUTOR_OP_H_
#define GRAPH_SYSTEMS_CORE_COMPONENTS_EXECUTOR_OP_H_

#include <chrono>
#include <memory>
#include <thread>
#include <type_traits>

#include "apis/pie.h"
#include "common/config.h"
#include "common/multithreading/thread_pool.h"
#include "components/component.h"
#include "scheduler/message_hub.h"
#include "util/logging.h"

namespace sics::graph::core::components {

class ExecutorOp {
 public:
  ExecutorOp() : task_runner_(common::Configurations::Get()->parallelism){};
  ExecutorOp(scheduler::MessageHub* hub)
      : execute_q_(hub->get_executor_queue()),
        response_q_(hub->get_response_queue()),
        task_runner_(common::Configurations::Get()->parallelism) {
    in_memory_time_ = common::Configurations::Get()->in_memory;
  }
  void Init(scheduler::MessageHub* hub) {
    execute_q_ = hub->get_executor_queue();
    response_q_ = hub->get_response_queue();
  }
  ~ExecutorOp() = default;

  void Start() {
    thread_ = std::make_unique<std::thread>([this]() {
      while (true) {
        scheduler::ExecuteMessage message = execute_q_->PopOrWait();
        if (message.terminated) {
          LOG_INFO("*** Executor is signaled termination ***");
          break;
        }

        LOGF_INFO("Executor starts executing subgraph {}", message.graph_id);
        // TODO: execute api logic
        switch (message.execute_type) {
          case scheduler::ExecuteType::kDeserialize: {
            LOGF_INFO("Executor: Deserialize graph {}", message.graph_id);
            data_structures::Serializable* graph = message.graph;
            graph->Deserialize(task_runner_,
                               std::unique_ptr<data_structures::Serialized>(
                                   message.serialized));
            message.response_serializable = graph;
            break;
          }
          case scheduler::ExecuteType::kPEval:
            LOGF_INFO("Executor: PEval graph {}", message.graph_id);
            if (in_memory_time_) start_time_ = std::chrono::system_clock::now();
            message.app->PEval();
            if (in_memory_time_) end_time_ = std::chrono::system_clock::now();
            break;
          case scheduler::ExecuteType::kIncEval:
            LOGF_INFO("Executor: kIncEval graph {}", message.graph_id);
            message.app->IncEval();
            break;
          case scheduler::ExecuteType::kSerialize:
            LOGF_INFO("Executor: Serialized graph {}", message.graph_id);
            // Set serialized graph to message for write back to disk.
            message.serialized =
                message.graph->Serialize(task_runner_).release();
            break;
        }
        LOGF_INFO("Executor completes executing subgraph {}", message.graph_id);
        response_q_->Push(scheduler::Message(message));
      }
    });
  }

  void StopAndJoin() {
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

  common::TaskRunner* GetTaskRunner() { return &task_runner_; }

 private:
  scheduler::ExecutorQueue* execute_q_;
  scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;
  common::ThreadPool task_runner_;

  bool in_memory_time_ = false;
  std::chrono::time_point<std::chrono::system_clock> start_time_;
  std::chrono::time_point<std::chrono::system_clock> end_time_;
};

}  // namespace sics::graph::core::components

#endif  // GRAPH_SYSTEMS_CORE_COMPONENTS_EXECUTOR_OP_H_
