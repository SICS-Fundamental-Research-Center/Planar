#include "components/executor.h"

namespace sics::graph::core::components {
void Executor::Start() {
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
          graph->Deserialize(
              task_runner_,
              std::unique_ptr<data_structures::Serialized>(message.serialized));
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
          message.serialized = message.graph->Serialize(task_runner_).release();
          break;
      }
      LOGF_INFO("Executor completes executing subgraph {}", message.graph_id);
      response_q_->Push(scheduler::Message(message));
    }
  });
}
}  // namespace sics::graph::core::components