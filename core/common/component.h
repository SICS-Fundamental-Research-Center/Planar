#ifndef GRAPH_SYSTEMS_COMPONENT_H
#define GRAPH_SYSTEMS_COMPONENT_H

namespace sics::graph::core::common {

class Component {
 public:
  virtual ~Component() = default;

  virtual void Start() = 0;
  virtual void StopAndJoin() = 0;
};

}  // namespace sics::graph::core::common

#endif  // GRAPH_SYSTEMS_COMPONENT_H
