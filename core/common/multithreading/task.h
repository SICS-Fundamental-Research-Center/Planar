#ifndef GRAPH_SYSTEMS_TASK_H
#define GRAPH_SYSTEMS_TASK_H

#include <functional>
#include <vector>


namespace sics::graph::core::common {

// Alias type name for an executable function.
typedef std::function<void()> Task;

// Alias type name for an array of executable functions.
typedef std::vector<Task> TaskPackage;


} // namespace sics::graph::core::common

#endif  // GRAPH_SYSTEMS_TASK_H
