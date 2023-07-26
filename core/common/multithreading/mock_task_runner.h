#ifndef CORE_COMMON_MULTITHREADING_MOCK_TASKRUNNER_H_
#define CORE_COMMON_MULTITHREADING_MOCK_TASKRUNNER_H_

#include "task_runner.h"

namespace sics::graph::core::common {

class MockTaskRunner : public TaskRunner {
 public:
  void SubmitAsync(Task&& task) override {}
  void SubmitAsync(Task&& task, std::function<void()> callback) override {}
  void SubmitAsync(const TaskPackage& tasks) override {}
  void SubmitAsync(const TaskPackage& tasks,
                   std::function<void()> callback) override {}
  void SubmitSync(Task&& task) override {}
  void SubmitSync(const TaskPackage& tasks) override {}
  size_t GetParallelism() const override { return 0; }
};

}  // namespace sics::graph::core::common

#endif  // CORE_COMMON_MULTITHREADING_MOCK_TASKRUNNER_H_
