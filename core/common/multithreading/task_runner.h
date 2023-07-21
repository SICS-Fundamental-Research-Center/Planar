//
// Created by Shuhao Liu on 2023-07-18.
//

#ifndef GRAPH_SYSTEMS_TASKRUNNER_H
#define GRAPH_SYSTEMS_TASKRUNNER_H

#include "task.h"


namespace sics::graph::core::common {

class TaskRunner {
 public:
  void SubmitSync(TaskPackage tasks);
};

}  // namespace sics::graph::core::common

#endif  // GRAPH_SYSTEMS_TASKRUNNER_H
