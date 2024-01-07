#ifndef GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_BASE_H_
#define GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_BASE_H_

namespace sics::graph::nvme::apis {

class BlockModelBase {
 public:
  virtual ~BlockModelBase() = default;

  virtual void Compute() = 0;

  //  virtual void MapVertex() = 0;

  //  virtual void MapEdge() = 0;

  //  virtual void MapAndMutateEdge() = 0;

 protected:
};

}  // namespace sics::graph::nvme::apis

#endif  // GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_BASE_H_
