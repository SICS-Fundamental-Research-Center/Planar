#include "tools/graph_partitioner/partitioner/hash_based_edgecut.h"

#include <filesystem>
#include <string>

#include <folly/hash/Hash.h>

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
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexLabel;
using sics::graph::core::data_structures::GraphMetadata;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::atomic::WriteMax;
using sics::graph::core::util::atomic::WriteMin;
using sics::graph::tools::common::Edge;
using sics::graph::tools::common::EdgelistMetadata;
using sics::graph::tools::common::GraphFormatConverter;
using sics::graph::tools::common::kIncomingOnly;
using sics::graph::tools::common::kOutgoingOnly;
using sics::graph::tools::common::kUnconstrained;
using sics::graph::tools::common::StoreStrategy;
using Vertex = sics::graph::core::data_structures::graph::ImmutableCSRVertex;
using sics::graph::tools::common::StoreStrategy2Enum;
using std::filesystem::create_directory;
using std::filesystem::exists;

VertexID HashBasedEdgeCutPartitioner::GetBucketID(VertexID vid,
                                                  VertexID n_bucket,
                                                  size_t n_vertices = 0) const {
  if (n_vertices != 0)
    return vid / ceil((double) n_vertices / (double) n_bucket);
  else
    return fnv64_append_byte(vid, 3) % n_bucket;
}

void HashBasedEdgeCutPartitioner::RunPartitioner() {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();
  task_package.reserve(parallelism);
  std::mutex mtx;

  // Load Yaml node (Edgelist metadata).
  YAML::Node input_node;
  input_node = YAML::LoadFile(input_path_ + "meta.yaml");
  auto edgelist_metadata = input_node["EdgelistBin"].as<EdgelistMetadata>();

  // Create Edgelist Graph.
  auto aligned_max_vid = (((edgelist_metadata.max_vid + 1) >> 6) << 6) + 64;
  auto bitmap = Bitmap(aligned_max_vid);
  auto buffer_edges = new Edge[edgelist_metadata.num_edges]();
  std::ifstream input_stream(input_path_ + "edgelist.bin", std::ios::binary);
  if (!input_stream.is_open()) LOG_FATAL("Cannot open edgelist.bin");

  input_stream.read(reinterpret_cast<char*>(buffer_edges),
                    sizeof(Edge) * edgelist_metadata.num_edges);
  input_stream.close();

  // Generate vertices.
  auto num_inedges_by_vid = new VertexID[aligned_max_vid]();
  auto num_outedges_by_vid = new VertexID[aligned_max_vid]();

  auto visited = Bitmap(aligned_max_vid);
  VertexID max_vid = 0, min_vid = MAX_VERTEX_ID;

  // Compute max_vid, min_vid. And get degree for each vertex.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i]() {
      for (EdgeIndex j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto e = buffer_edges[j];
        visited.SetBit(e.src);
        visited.SetBit(e.dst);
        WriteAdd(num_inedges_by_vid + e.dst, (VertexID) 1);
        WriteAdd(num_outedges_by_vid + e.src, (VertexID) 1);
        WriteMax(&max_vid, e.src);
        WriteMax(&max_vid, e.dst);
        WriteMin(&min_vid, e.src);
        WriteMin(&min_vid, e.dst);
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  std::vector<Vertex> vertices;
  vertices.resize(aligned_max_vid);

  // malloc space for each vertex.
  EdgeIndex count_in_edges = 0, count_out_edges = 0;
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i]() {
      for (VertexID j = i; j < aligned_max_vid; j += parallelism) {
        if (!visited.GetBit(j)) continue;
        vertices.at(j).vid = j;
        vertices.at(j).indegree = num_inedges_by_vid[j];
        vertices.at(j).outdegree = num_outedges_by_vid[j];
        vertices.at(j).incoming_edges = new VertexID[num_inedges_by_vid[j]]();
        vertices.at(j).outgoing_edges = new VertexID[num_outedges_by_vid[j]]();
        WriteAdd(&count_in_edges, (EdgeIndex) num_inedges_by_vid[j]);
        WriteAdd(&count_out_edges, (EdgeIndex) num_outedges_by_vid[j]);
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete[] num_inedges_by_vid;
  delete[] num_outedges_by_vid;

  // Fill edges.
  EdgeIndex* offset_in_edges = new EdgeIndex[aligned_max_vid]();
  EdgeIndex* offset_out_edges = new EdgeIndex[aligned_max_vid]();

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (EdgeIndex j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto e = buffer_edges[j];
        auto offset_out = __sync_fetch_and_add(offset_out_edges + e.src, 1);
        auto offset_in = __sync_fetch_and_add(offset_in_edges + e.dst, 1);
        vertices.at(e.src).outgoing_edges[offset_out] = e.dst;
        vertices.at(e.dst).incoming_edges[offset_in] = e.src;
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  delete[] buffer_edges;
  delete[] offset_in_edges;
  delete[] offset_out_edges;

  // Construct subgraphs.
  std::vector<std::vector<Vertex>> vertex_buckets;

  vertex_buckets.resize(n_partitions_);
  for (VertexID i = 0; i < n_partitions_; i++)
    vertex_buckets[i].reserve(aligned_max_vid / n_partitions_);

  // Fill buckets.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism, n_partitions = n_partitions_]() {
      for (VertexID j = i; j < aligned_max_vid; j += parallelism) {
        if (!visited.GetBit(j)) continue;
        auto gid = GetBucketID(vertices.at(j).vid, n_partitions,
                               edgelist_metadata.num_vertices);
        std::lock_guard<std::mutex> lck(mtx);
        vertex_buckets[gid].emplace_back(std::move(vertices.at(j)));
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (VertexID j = i; j < n_partitions_; j += parallelism) {
        std::sort(vertex_buckets[j].begin(), vertex_buckets[j].end(),
                  [](const auto& l, const auto& r) { return l.vid < r.vid; });
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  // Write the subgraphs to disk
  GraphFormatConverter graph_format_converter(output_path_);
  GraphMetadata graph_metadata;
  graph_metadata.set_num_vertices(edgelist_metadata.num_vertices);
  graph_metadata.set_num_edges(edgelist_metadata.num_edges);
  graph_metadata.set_num_subgraphs(n_partitions_);
  graph_metadata.set_max_vid(max_vid);
  graph_metadata.set_min_vid(min_vid);

  LOG_INFO("Writing the subgraphs to disk");
  graph_format_converter.WriteSubgraph(vertex_buckets, graph_metadata,
                                       store_strategy_);

  LOG_INFO("Finished writing the subgraphs to disk");
}

}  // namespace sics::graph::tools::partitioner
