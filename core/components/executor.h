#ifndef GRAPH_SYSTEMS_EXECUTER_H
#define GRAPH_SYSTEMS_EXECUTER_H

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

class Executor : public Component {
 public:
  Executor(scheduler::MessageHub* hub)
      : execute_q_(hub->get_executor_queue()),
        response_q_(hub->get_response_queue()),
        task_runner_(common::Configurations::Get()->parallelism) {
    in_memory_time_ = common::Configurations::Get()->in_memory;
  }
  ~Executor() final = default;

  void Start() override;

  void StopAndJoin() override {
    scheduler::ExecuteMessage message;
    message.terminated = true;
    execute_q_->Push(message);
    // first stop the task_runner, then stop the Executor thread
    thread_->join();
    if (in_memory_time_) {
      LOGF_INFO("========== In memory time: {}s ==========",
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

#endif  // GRAPH_SYSTEMS_EXECUTER_H
