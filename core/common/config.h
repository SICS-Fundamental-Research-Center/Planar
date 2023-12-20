#ifndef GRAPH_SYSTEMS_CORE_COMMON_CONFIG_H_
#define GRAPH_SYSTEMS_CORE_COMMON_CONFIG_H_

#include <string>

namespace sics::graph::core::common {

enum VertexDataType {
  kVertexDataTypeUInt32 = 1,
  kVertexDataTypeUInt16,
};

enum PartitionType {
  PlanarVertexCut = 1,
  HashVertexCut,
  Vertex2DCut,
  EdgeCut,
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
  PartitionType partition_type = PlanarVertexCut;
  std::string root_path = "/testfile";
  VertexDataType vertex_data_type = kVertexDataTypeUInt32;
  bool edge_mutate = false;
  bool in_memory = false;
  int limits = 0;
  bool short_cut = true;
  uint32_t vertex_data_size = 4;
  size_t memory_size = 64 * 1024;
  ApplicationType application = WCC;
  // for coloring
  uint32_t rand_max = 10000;
  // for sssp
  uint32_t source = 0;
  bool ASP = false;
  // for mst
  bool fast = false;
  // for wcc
  bool wcc_ASP = false;

  bool threefour_mode = false;

  bool group = false;

 private:
  Configurations() = default;
  inline static Configurations* instance_ = nullptr;
};
}  // namespace sics::graph::core::common

#endif  // GRAPH_SYSTEMS_CORE_COMMON_CONFIG_H_
