#ifndef CORE_COMMON_MULTITHREADING_TASK_H_
#define CORE_COMMON_MULTITHREADING_TASK_H_

#include <functional>
#include <vector>


namespace xyz::graph::core::common {

// Alias type name for an executable function.
typedef std::function<void()> Task;

// Alias type name for an array of executable functions.
typedef std::vector<Task> TaskPackage;


} // namespace xyz::graph::core::common

#endif  // CORE_COMMON_MULTITHREADING_TASK_H_
