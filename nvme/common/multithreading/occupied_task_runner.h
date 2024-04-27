#ifndef GRAPH_SYSTEMS_NVME_COMMON_MULTITHREADING_OCCUPIED_TASK_RUNNER_H_
#define GRAPH_SYSTEMS_NVME_COMMON_MULTITHREADING_OCCUPIED_TASK_RUNNER_H_

#include "core/common/multithreading/task_runner.h"

namespace sics::graph::nvme::common {

using Task = core::common::Task;
using TaskPackage = core::common::TaskPackage;

class OccupiedPool final : public core::common::TaskRunner {
 public:
  explicit OccupiedPool() = default;
  ~OccupiedPool() = default;

  void SubmitAsync(Task&& task) override {}
  void SubmitAsync(Task&& task, std::function<void()> callback) override {}
  void SubmitAsync(const TaskPackage& tasks) override {}
  void SubmitAsync(const TaskPackage& tasks,
                   std::function<void()> callback) override {}

  // Submit a single task (resp. a package of tasks) for execution.
  // The call will block until all submitted tasks are completed.
  void SubmitSync(Task&& task) override {}
  void SubmitSync(const TaskPackage& tasks) override {}

  // Get the current parallelism for running tasks.
  //
  // It is useful for task submitters to estimate the currently allowed
  // concurrency, and to batch tiny tasks into larger serial ones while
  // making no compromise on utilization of allocated resources.
  [[nodiscard]] size_t GetParallelism() const { return 0; }
};

}  // namespace sics::graph::nvme::common

#endif  // GRAPH_SYSTEMS_NVME_COMMON_MULTITHREADING_OCCUPIED_TASK_RUNNER_H_
