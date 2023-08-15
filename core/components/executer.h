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

  void Start() override;

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
