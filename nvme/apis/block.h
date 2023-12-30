#ifndef GRAPH_SYSTEMS_NVME_APIS_BLOCK_H_
#define GRAPH_SYSTEMS_NVME_APIS_BLOCK_H_

namespace sics::graph::nvme::apis {

class BlockModel {
 public:
  virtual ~BlockModel() = default;

  virtual void Compute() = 0;
};

}  // namespace sics::graph::nvme::apis

#endif  // GRAPH_SYSTEMS_NVME_APIS_BLOCK_H_
