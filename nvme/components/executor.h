#ifndef GRAPH_SYSTEMS_NVME_COMPONENTS_EXECUTOR_H_
#define GRAPH_SYSTEMS_NVME_COMPONENTS_EXECUTOR_H_

#include <chrono>
#include <memory>
#include <thread>
#include <type_traits>

#include "core/apis/pie.h"
#include "core/common/config.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/common/types.h"
#include "core/components/component.h"
#include "core/util/logging.h"
#include "nvme/common/config.h"
#include "nvme/components/component.h"
#include "nvme/data_structures/graph/pram_block.h"
#include "nvme/scheduler/message_hub.h"

namespace sics::graph::nvme::components {

class Executor : public Component {
  using GraphID = core::common::GraphID;
  using VertexID = core::common::VertexID;
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using FuncVertex = core::common::FuncVertex;
  using FuncEdge = core::common::FuncEdge;
  using FuncEdgeAndMutate = core::common::FuncEdgeAndMutate;
  using FuncEdgeMutate = core::common::FuncEdgeMutate;

 public:
  Executor(scheduler::MessageHub* hub)
      : execute_q_(hub->get_executor_queue()),
        response_q_(hub->get_response_queue()),
        task_runner_(core::common::Configurations::Get()->parallelism),
        parallelism_(core::common::Configurations::Get()->parallelism),
        task_package_factor_(
            core::common::Configurations::Get()->task_package_factor) {
    in_memory_time_ = core::common::Configurations::Get()->in_memory;
    edge_mutate_ = core::common::Configurations::Get()->edge_mutate;
    task_size_ = core::common::Configurations::Get()->task_size;
  }
  ~Executor() final = default;

  void Start() override {
    thread_ = std::make_unique<std::thread>([this]() {
      LOG_INFO("*** Executor starts ***");
      while (true) {
        scheduler::ExecuteMessage message = execute_q_->PopOrWait();
        if (message.terminated) {
          // LOG_INFO("*** Executor is signaled termination ***");
          break;
        }
      }
    });
  }

  void StopAndJoin() override {
    scheduler::ExecuteMessage message;
    message.terminated = true;
    execute_q_->Push(message);
    // first stop the task_runner, then stop the Executor thread
    thread_->join();
    LOG_INFO("*** Executor stops ***");
    //    if (in_memory_time_) {
    //      LOGF_INFO("========== In memory time: {} s ==========",
    //                std::chrono::duration<double>(end_time_ -
    //                start_time_).count());
    //    }
  }

  core::common::TaskRunner* GetTaskRunner() { return &task_runner_; }

 private:
  scheduler::ExecutorQueue* execute_q_;
  scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;
  core::common::ThreadPool task_runner_;

  bool in_memory_time_ = true;

  // configs
  const uint32_t parallelism_;
  const uint32_t task_package_factor_;
  bool edge_mutate_ = false;
  uint32_t task_size_ = 500000;
};

}  // namespace sics::graph::nvme::components

#endif  // GRAPH_SYSTEMS_NVME_COMPONENTS_EXECUTOR_H_
