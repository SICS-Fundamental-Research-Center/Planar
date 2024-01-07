#ifndef GRAPH_SYSTEMS_NVME_COMPONENTS_COMPONENT_H_
#define GRAPH_SYSTEMS_NVME_COMPONENTS_COMPONENT_H_

namespace sics::graph::nvme::components {

class Component {
 public:
  virtual ~Component() = default;

  virtual void Start() = 0;
  virtual void StopAndJoin() = 0;
};

}  // namespace sics::graph::nvme::components

#endif  // GRAPH_SYSTEMS_NVME_COMPONENTS_COMPONENT_H_
