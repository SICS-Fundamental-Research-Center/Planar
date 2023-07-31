#include "thread_pool.h"

namespace sics::graph::core::common {

ThreadPool::ThreadPool(uint32_t num_threads)
    : internal_pool_((unsigned int)num_threads) {}

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
  auto parallelism = internal_pool_.numThreads();
  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_threads(parallelism);

  for (size_t tid = 0; tid < parallelism; tid++) {
    internal_pool_.add(
        [this, tid, &finish_cv, &pending_threads, &tasks, &parallelism]() {
          for (size_t i = tid; i < tasks.size(); i += parallelism) {
            LOG_INFO(i, "/", tasks.size(), "/", parallelism);
            auto task = tasks.at(i);
            task();
          }
          if (pending_threads.fetch_sub(1) == 1) finish_cv.notify_all();
        });
  }
  finish_cv.wait(lck, [&] { return pending_threads.load() == 0; });
}

size_t ThreadPool::GetParallelism() const {
  return internal_pool_.numThreads();
}

void ThreadPool::StopAndJoin() {
  internal_pool_.stop();
  internal_pool_.join();
}

}  // namespace sics::graph::core::common
