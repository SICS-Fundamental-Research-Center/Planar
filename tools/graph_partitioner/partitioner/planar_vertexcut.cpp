#include <algorithm>
#include <filesystem>
#include <string>

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/hash/Hash.h>

#include "tools/graph_partitioner/partitioner/planar_vertexcut.h"
#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/common/types.h"
#include "core/util/atomic.h"
#include "core/util/logging.h"
#include "tools/common/data_structures.h"
#include "tools/common/io.h"

namespace sics::graph::tools::partitioner {

using folly::ConcurrentHashMap;
using folly::hash::fnv64_append_byte;
using sics::graph::core::common::Bitmap;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexLabel;
using sics::graph::core::data_structures::GraphMetadata;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::atomic::WriteMax;
using sics::graph::core::util::atomic::WriteMin;
using sics::graph::tools::common::Edge;
using sics::graph::tools::common::EdgelistMetadata;
using sics::graph::tools::common::Edges;
using sics::graph::tools::common::GraphFormatConverter;
using sics::graph::tools::common::kIncomingOnly;
using sics::graph::tools::common::kOutgoingOnly;
using sics::graph::tools::common::kUnconstrained;
using sics::graph::tools::common::StoreStrategy;
using Vertex = sics::graph::core::data_structures::graph::ImmutableCSRVertex;
using sics::graph::tools::common::StoreStrategy2Enum;
using std::filesystem::create_directory;
using std::filesystem::exists;

void PlanarVertexCutPartitioner::RunPartitioner() {
  // TODO (hsiaoko): to implement RunPartitioner
}

std::list<Edges*> PlanarVertexCutPartitioner::SortBFSBranch(Edges& edges,
                                                              VertexID r) {
  std::list<Edges*> branches;
  // TODO (hsiaoko): to implement SortBFSBRanch
  return branches;
}

}  // namespace sics::graph::tools::partitioner
