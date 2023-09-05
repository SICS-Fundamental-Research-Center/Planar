#ifndef GRAPH_SYSTEMS_CORE_COMMON_CONFIG_H_
#define GRAPH_SYSTEMS_CORE_COMMON_CONFIG_H_

#include <string>

namespace sics::graph::core::common {

enum VertexDataType {
  kVertexDataTypeUInt32 = 1,
  kVertexDataTypeUInt16,
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
  int max_task_package = 4000;
  int parallelism = 1;
  std::string partition_type = "VertexCut";
  std::string root_path = "/testfile";
  VertexDataType vertex_data_type = kVertexDataTypeUInt32;
  bool edge_mutate = false;

 private:
  Configurations() = default;
  inline static Configurations* instance_ = nullptr;
};
}  // namespace sics::graph::core::common

#endif  // GRAPH_SYSTEMS_CORE_COMMON_CONFIG_H_
