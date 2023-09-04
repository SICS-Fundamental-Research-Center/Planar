#include "components/executor.h"

namespace sics::graph::core::components {
void Executor::Start() {
  thread_ = std::make_unique<std::thread>([this]() {
    while (true) {
      scheduler::ExecuteMessage message = execute_q_->PopOrWait();
      if (message.terminated) {
        LOG_INFO("*** Executer is signaled termination ***");
        break;
      }

      LOGF_INFO("Executer starts executing subgraph {}", message.graph_id);
      // TODO: execute api logic
      switch (message.execute_type) {
        case scheduler::ExecuteType::kDeserialize: {
          data_structures::Serializable* graph = message.graph;
          graph->Deserialize(
              task_runner_,
              std::unique_ptr<data_structures::Serialized>(message.serialized));
          message.response_serializable = graph;
          break;
        }
        case scheduler::ExecuteType::kPEval:
          message.app->PEval();
          break;
        case scheduler::ExecuteType::kIncEval:
          message.app->IncEval();
          break;
        case scheduler::ExecuteType::kSerialize:
          //            message.graph->Deserialize();
          break;
      }
      LOGF_INFO("Executer completes executing subgraph {}", message.graph_id);
      response_q_->Push(scheduler::Message(message));
    }
  });
}
}  // namespace sics::graph::core::components