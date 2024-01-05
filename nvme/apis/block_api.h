#ifndef GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_
#define GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_

#include <functional>
#include <type_traits>
#include <vector>

#include "core/common/multithreading/task_runner.h"
#include "core/common/types.h"
#include "core/data_structures/serializable.h"
#include "core/util/logging.h"
#include "nvme/apis/block_api_base.h"

namespace sics::graph::nvme::apis {

using core::data_structures::Serializable;

enum BlockModelApiType {
  kVertex = 1,
  kEdge,
  kMutateEdge,
};

template <typename GraphType>
class BlockModel : public BlockModelBase {
  static_assert(std::is_base_of<Serializable, GraphType>::value,
                "GraphType must be a subclass of Serializable");
  using VertexData = typename GraphType::VertexData;
  using EdgeData = typename GraphType::EdgeData;

  using GraphID = core::common::GraphID;
  using VertexID = core::common::VertexID;
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;

 public:
  BlockModel() {}

  ~BlockModel() override = default;

 protected:
  void Compute() override {
    switch (type_) {}
  }

 protected:
  BlockModelApiType type_ = kVertex;

  core::common::TaskRunner* runner_;

  GraphType* graph_ = nullptr;

  int round_ = 0;

  // configs
  const uint32_t parallelism_;
};

}  // namespace sics::graph::nvme::apis

#endif  // GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_
