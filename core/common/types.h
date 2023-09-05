#ifndef CORE_COMMON_TYPES_H_
#define CORE_COMMON_TYPES_H_

#include <gflags/gflags.h>

#include <cstdint>
#include <limits>
#include <string>

namespace sics::graph::core::common {

typedef uint32_t GraphID;  // uint32_t: 0 ~ 4,294,967,295
typedef uint32_t VertexID;
typedef uint32_t VertexIndex;
typedef uint32_t VertexLabel;
typedef uint32_t VertexCount;
typedef uint64_t EdgeIndex;

#define MAX_VERTEX_ID std::numeric_limits<VertexID>::max()
#define INVALID_GRAPH_ID \
  std::numeric_limits<sics::graph::core::common::GraphID>::max()

static int subgraph_limits = 1;  // used only for test.

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
  VertexDataType vertex_type = kVertexDataTypeUInt32;
  bool edge_mutate = false;

 private:
  Configurations() = default;
  static Configurations* instance_;
};

}  // namespace sics::graph::core::common

#endif  // CORE_COMMON_TYPES_H_
