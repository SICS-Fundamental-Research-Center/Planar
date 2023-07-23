#ifndef GRAPH_SYSTEMS_THREADPOOL_H
#define GRAPH_SYSTEMS_THREADPOOL_H

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "task_runner.h"


namespace sics::graph::core::common {

// A wrapper of `folly::CPUThreadPoolExecutor` class, adapting it to the
// `TaskRunner` interface.
class ThreadPool final : public TaskRunner {
 public:
  // Parameter `num_threads` determines the number of threads in the pool.
  explicit ThreadPool(uint32_t num_threads);
  ~ThreadPool() = default;

  // Submit a single task (resp. a package of tasks) for execution.
  // The call will return immediately.
  //
  // One may provide a callback function to invoke after all submitted tasks
  // are completed.
  void SubmitAsync(Task&& task) override;
  void SubmitAsync(Task&& task, std::function<void ()> callback) override;
  void SubmitAsync(const TaskPackage& tasks) override;
  void SubmitAsync(const TaskPackage& tasks,
                   std::function<void ()> callback) override;

  // Submit a single task (resp. a package of tasks) for execution.
  // The call will block until all submitted tasks are completed.
  void SubmitSync(Task&& task) override;
  void SubmitSync(const TaskPackage& tasks) override;

  // Get the total number of threads within the thread pool.
  size_t GetParallelism() const override;

  // Stop the thread pool and join all threads.
  void StopAndJoin();

 private:
  folly::CPUThreadPoolExecutor internal_pool_;
};

}  // namespace sics::graph::core::common

#endif  // GRAPH_SYSTEMS_THREADPOOL_H
