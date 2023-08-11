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

class Executer : public Component {
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
        switch (message.execute_type) {
          case scheduler::ExecuteType::kDeserialize:

            break;
          case scheduler::ExecuteType::kPEval:

            break;
          case scheduler::ExecuteType::kIncEval:

            break;
          case scheduler::ExecuteType::kSerialize:

            break;
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
  scheduler::ExecutorQueue* execute_q_;
  scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;
};

}  // namespace sics::graph::core::components

#endif  // GRAPH_SYSTEMS_EXECUTER_H
