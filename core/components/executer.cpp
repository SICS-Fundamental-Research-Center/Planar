#include "components/executer.h"

namespace sics::graph::core::components {
void Executer::Start() {
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
          //            data_structures::Serializable* graph;
          //            graph->Deserialize()
          //            message.response_serializable = graph;
          break;
        }
        case scheduler::ExecuteType::kPEval:
          message.api->PEval();
          break;
        case scheduler::ExecuteType::kIncEval:
          message.api->IncEval();
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