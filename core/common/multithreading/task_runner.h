#ifndef GRAPH_SYSTEMS_TASKRUNNER_H
#define GRAPH_SYSTEMS_TASKRUNNER_H

#include "task.h"


namespace sics::graph::core::common {

// An interface class, which features a series of `Submit` functions
// that accept an array `Task`s and run them as a batch.
class TaskRunner {
 public:
  // Submit a single task (resp. a package of tasks) for execution.
  // The call will return immediately.
  //
  // One may provide a callback function to invoke after all submitted tasks
  // are completed.
  virtual void SubmitAsync(Task&& task) = 0;
  virtual void SubmitAsync(Task&& task, std::function<void ()> callback) = 0;
  virtual void SubmitAsync(const TaskPackage& tasks) = 0;
  virtual void SubmitAsync(const TaskPackage& tasks,
                           std::function<void ()> callback) = 0;

  // Submit a single task (resp. a package of tasks) for execution.
  // The call will block until all submitted tasks are completed.
  virtual void SubmitSync(Task&& task) = 0;
  virtual void SubmitSync(const TaskPackage& tasks) = 0;

  // Get the current parallelism for running tasks.
  //
  // It is useful for task submitters to estimate the currently allowed
  // concurrency, and to batch tiny tasks into larger serial ones while
  // making no compromise on utilization of allocated resources.
  [[nodiscard]]
  virtual size_t GetParallelism() const = 0;
};

}  // namespace sics::graph::core::common

#endif  // GRAPH_SYSTEMS_TASKRUNNER_H
