#ifndef GRAPH_SYSTEMS_COMPONENT_H
#define GRAPH_SYSTEMS_COMPONENT_H

namespace xyz::graph::core::components {

class Component {
 public:
  virtual ~Component() = default;

  virtual void Start() = 0;
  virtual void StopAndJoin() = 0;
};

}  // namespace xyz::graph::core::components

#endif  // GRAPH_SYSTEMS_COMPONENT_H
