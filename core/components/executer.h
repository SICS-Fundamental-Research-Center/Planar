#ifndef GRAPH_SYSTEMS_EXECUTER_H
#define GRAPH_SYSTEMS_EXECUTER_H

#include <memory>
#include <thread>
#include <type_traits>

#include "apis/pie.h"
#include "components/component.h"
#include "io/reader_writer.h"
#include "scheduler/message_hub.h"
#include "util/logging.h"

namespace sics::graph::core::components {

template <typename PIEApp>
class Executer : public Component {
 protected:
  static_assert(std::is_base_of<apis::PIE, PIEApp>::value,
                "PIEApp must be a subclass of PIEApp");

 public:
  Executer(scheduler::MessageHub* hub)
      : execute_q_(hub->get_executor_queue()),
        response_q_(hub->get_response_queue()) {}
  ~Executer() final = default;

  void Start() override {
    thread_ = std::make_unique<std::thread>([this]() {
      while (true) {
        scheduler::ExecuteMessage message = execute_q_->PopOrWait();
        if (message.terminated) {
          LOG_INFO("*** Executer is signaled termination ***");
          break;
        }

        LOGF_INFO("Executer starts executing subgraph {}", message.graph_id);
        // TODO: execute api logic
        if (message.execute_type == scheduler::ExecuteType::kPEval) {
          app_->PEval();
        } else if (message.execute_type == scheduler::ExecuteType::kIncEval) {
          app_->IncEval();
        }
        LOGF_INFO("Executer completes executing subgraph {}", message.graph_id);
        response_q_->Push(scheduler::Message(message));
      }
    });
  }

  void StopAndJoin() override {
    scheduler::ExecuteMessage message;
    message.terminated = true;
    execute_q_->Push(message);
    thread_->join();
  }

 private:
  PIEApp app_;

  scheduler::ExecutorQueue* execute_q_;
  scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;
};

}  // namespace sics::graph::core::components

#endif  // GRAPH_SYSTEMS_EXECUTER_H
