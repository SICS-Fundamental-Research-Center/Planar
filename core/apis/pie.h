#ifndef GRAPH_SYSTEMS_PIE_H
#define GRAPH_SYSTEMS_PIE_H

#include "common/types.h"

namespace sics::graph::core::apis {

// PIE interfaces for graph applications.
class PIE {
 public:
  virtual ~PIE() = default;

  virtual void PEval() = 0;

  virtual void IncEval() = 0;

  virtual void Assemble() = 0;

  virtual void SetCurrentGid(common::GraphID gid) = 0;

  virtual size_t IsActive() = 0;

  virtual void SetRound(int round) = 0;
};

}  // namespace sics::graph::core::apis

#endif  // GRAPH_SYSTEMS_PIE_H
