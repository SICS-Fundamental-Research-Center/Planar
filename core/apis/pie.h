#ifndef GRAPH_SYSTEMS_PIE_H
#define GRAPH_SYSTEMS_PIE_H

namespace sics::graph::core::apis {

// PIE interfaces for graph applications.
class PIE {
 public:
  virtual ~PIE() = default;

  virtual void PEval() = 0;

  virtual void IncEval() = 0;

  virtual void Assemble() = 0;
};

}  // namespace sics::graph::core::apis

#endif  // GRAPH_SYSTEMS_PIE_H
