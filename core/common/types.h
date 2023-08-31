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

// TODO: struct configuration
struct Configurations {
  int max_task_package = 4000;
  int parallelism = 1;
  std::string partition_type = "VertexCut";
  std::string root_path = "/testfile";
  int subgraph_limits = 1;
};

#define MAX_VERTEX_ID std::numeric_limits<VertexID>::max()
#define INVALID_GRAPH_ID \
  std::numeric_limits<sics::graph::core::common::GraphID>::max()

// used for global flags == gflags
static int kDefaultMaxTaskPackage = 4000;
static int kDefaultParallelism = 1;
static std::string kPartitionType = "VertexCut";
static std::string kDefaultRootPath = "/testfile";

static Configurations configs;

}  // namespace sics::graph::core::common

#endif  // CORE_COMMON_TYPES_H_
