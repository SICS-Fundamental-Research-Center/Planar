#include "tools/graph_partitioner/partitioner/csr_based_planar_vertexcut.h"

#include <algorithm>
#include <filesystem>
#include <string>

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/hash/Hash.h>

#include "core/common/multithreading/thread_pool.h"
#include "core/common/types.h"
#include "core/io/csr_reader.h"
#include "core/util/atomic.h"
#include "core/util/logging.h"
#include "tools/common/data_structures.h"
#include "tools/common/io.h"

#ifdef TBB_FOUND
#include <execution>
#endif

#include "sys/sysinfo.h"
#include "sys/types.h"

namespace sics::graph::tools::partitioner {

using Vertex = sics::graph::core::data_structures::graph::ImmutableCSRVertex;
using folly::hash::fnv64_append_byte;
using sics::graph::core::common::Bitmap;
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::ThreadPool;
using sics::graph::core::common::VertexID;
using sics::graph::core::data_structures::GraphMetadata;
using sics::graph::tools::common::StoreStrategy2Enum;
using std::filesystem::create_directory;
using std::filesystem::exists;
using Edge = sics::graph::tools::common::Edge;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::data_structures::GraphMetadata;
using sics::graph::core::data_structures::SubgraphMetadata;
using sics::graph::core::data_structures::graph::ImmutableCSRGraph;
using sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using sics::graph::core::io::CSRReader;
using sics::graph::core::scheduler::ReadMessage;
using sics::graph::core::scheduler::WriteMessage;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::atomic::WriteMax;
using sics::graph::core::util::atomic::WriteMin;
using sics::graph::tools::common::Edges;
using sics::graph::tools::common::GraphFormatConverter;

void CSRBasedPlanarVertexCutPartitioner::RunPartitioner() {
  // TODO (hsiaoko): will submit in next PR.
}

std::list<std::list<Edge>> CSRBasedPlanarVertexCutPartitioner::SortBFSBranch(
    size_t minimum_n_edges_of_a_branch, const ImmutableCSRGraph& graph) {
  // TODO (hsiaoko): will submit in next PR.
  std::list<std::list<Edge>> out;
  return out;
}

std::list<std::list<Edge>>
CSRBasedPlanarVertexCutPartitioner::MergedSortBFSBranch(
    size_t minimum_n_edges_of_a_branch, const ImmutableCSRGraph& graph) {
  // TODO (hsiaoko): will submit in next PR.
  std::list<std::list<Edge>> out;
  return out;
}

void CSRBasedPlanarVertexCutPartitioner::Redistributing(
    GraphID expected_n_of_branches,
    std::list<std::list<Edge>>* sorted_list_of_branches) {
  // TODO (hsiaoko): will submit in next PR.
}

std::vector<Edges> CSRBasedPlanarVertexCutPartitioner::ConvertListofEdge2Edges(
    const std::list<std::list<Edge>>& list_of_branches) {
  // TODO (hsiaoko): will submit in next PR.
  std::vector<Edges> vec_edges;
  return vec_edges;
}

}  // namespace sics::graph::tools::partitioner