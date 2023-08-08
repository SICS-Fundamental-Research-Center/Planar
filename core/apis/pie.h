#ifndef GRAPH_SYSTEMS_PIE_H
#define GRAPH_SYSTEMS_PIE_H

#include <type_traits>

#include "common/multithreading/task_runner.h"
#include "data_structures/serializable.h"
#include "util/logging.h"

namespace sics::graph::core::apis {

// PIE interfaces for graph applications.
//
// Each application should be defined by inheriting the PIE with a
// concrete graph type, and implement the following methods:
// - PEval: the initial evaluation phase.
// - IncEval: the incremental evaluation phase.
// - Assemble: the final assembly phase.
template <typename GraphType>
class PIE {
  static_assert(
      std::is_base_of<data_structures::Serializable, GraphType>::value,
      "GraphType must be a subclass of Serializable");

 public:
  explicit PIE(common::TaskRunner* runner) : runner_(runner) {}

  virtual ~PIE() = default;

  // TODO: add UpdateStore as a parameter, so that PEval can save global
  //  messages to it.
  virtual void PEval(GraphType* graph) = 0;

  // TODO: add a const UpdateStore as input for the last round, and a mutable
  //  UpdateStore as output for the current round.
  virtual void IncEval(GraphType* graph) = 0;

  virtual void Assemble(GraphType* graph) = 0;

 protected:
  // TODO: implement this here. This function is not intended for overriden.
  void VApply(GraphType* graph) {
    LOG_WARN("VApply is not implemented");
  }

  // TODO: implement this here. This function is not intended for overriden.
  void EApply(GraphType* graph) {
    LOG_WARN("EApply is not implemented");
  }

 protected:
  common::TaskRunner* runner_;
};

}  // namespace sics::graph::core::apis

#endif  // GRAPH_SYSTEMS_PIE_H
