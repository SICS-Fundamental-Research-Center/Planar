#ifndef GRAPH_SYSTEMS_CORE_COMMON_CONFIG_H_
#define GRAPH_SYSTEMS_CORE_COMMON_CONFIG_H_

#include <string>

namespace sics::graph::core::common {

enum VertexDataType {
  kVertexDataTypeUInt32 = 1,
  kVertexDataTypeUInt16,
};

enum PartitionType {
  VertexCut = 1,
  EdgeCut,
  HybridCut,
  PlanarCut,
};

enum ApplicationType {
  WCC = 1,
  Coloring,
  Sssp,
  MST,
  RandomWalk,
  PageRank,
};

class Configurations {
 public:
  static const Configurations* Get() {
    if (instance_ == nullptr) {
      instance_ = new Configurations();
    }
    return instance_;
  }

  static Configurations* GetMutable() {
    if (instance_ == nullptr) {
      instance_ = new Configurations();
    }
    return instance_;
  }

  Configurations(const Configurations& rhs) = delete;
  Configurations& operator=(const Configurations& rhs) = delete;
  uint32_t task_package_factor = 100;
  uint32_t parallelism = 1;
  PartitionType partition_type = VertexCut;
  std::string root_path = "/testfile";
  VertexDataType vertex_data_type = kVertexDataTypeUInt32;
  bool edge_mutate = false;
  bool in_memory = false;
  uint32_t vertex_data_size = 4;
  size_t memory_size = 64 * 1024;
  ApplicationType application = WCC;
  // for coloring
  uint32_t rand_max = 10000;
  // for sssp
  uint32_t source = 0;

 private:
  Configurations() = default;
  inline static Configurations* instance_ = nullptr;
};
}  // namespace sics::graph::core::common

#endif  // GRAPH_SYSTEMS_CORE_COMMON_CONFIG_H_
