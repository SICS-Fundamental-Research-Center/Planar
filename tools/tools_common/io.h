#ifndef SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_IO_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_IO_H_

#include <filesystem>
#include <fstream>
#include <string>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "common/bitmap.h"
#include "common/multithreading/thread_pool.h"
#include "common/types.h"
#include "data_structures/graph_metadata.h"
#include "tools_common/data_structures.h"
#include "tools_common/types.h"
#include "tools_common/yaml_config.h"
#include "util/atomic.h"

namespace tools {

// Adapter for the CSR IO operations.
class CSRIOAdapter {
 using VertexID = sics::graph::core::common::VertexID;
 public:
  CSRIOAdapter(const std::string& output_root_path);

  bool WriteSubgraph(
      std::vector<folly::ConcurrentHashMap<VertexID, TMPCSRVertex>*> &,
  StoreStrategy);

 private:
  std::string output_root_path_;
};

}  // namespace tools

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_IO_H_
