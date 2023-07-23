#include "thread_pool.h"

namespace sics::graph::core::common {

ThreadPool::ThreadPool(uint32_t num_threads)
    : internal_pool_((unsigned int) num_threads) {}

void ThreadPool::SubmitAsync(Task&& task) {
  internal_pool_.add(std::move(task));
}

void ThreadPool::SubmitAsync(Task&& task, std::function<void ()> callback) {
  internal_pool_.add(std::move(task));
  // TODO: Implement the call back behavior.
}

void ThreadPool::SubmitAsync(const TaskPackage& tasks) {
  for (const auto& t : tasks) {
    internal_pool_.add(t);
  }
}

void ThreadPool::SubmitAsync(const TaskPackage& tasks,
                             std::function<void ()> callback) {
  for (const auto& t : tasks) {
    internal_pool_.add(t);
  }
  // TODO: Implement the call back behavior.
}

void ThreadPool::SubmitSync(Task&& task) {
  internal_pool_.add(std::move(task));
  // TODO: Implement the blocking behavior.
}

void ThreadPool::SubmitSync(const TaskPackage& tasks) {
  for (const auto& t : tasks) {
    internal_pool_.add(t);
  }
  // TODO: Implement the blocking behavior.
}

size_t ThreadPool::GetParallelism() const {
  return internal_pool_.numThreads();
}

void ThreadPool::StopAndJoin() {
  internal_pool_.stop();
  internal_pool_.join();
}

}  // namespace sics::graph::core::common
