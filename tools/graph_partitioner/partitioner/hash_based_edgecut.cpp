#include "tools/graph_partitioner/partitioner/hash_based_edgecut.h"

#include <array>
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
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexLabel;
using sics::graph::core::data_structures::GraphMetadata;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::atomic::WriteMax;
using sics::graph::core::util::atomic::WriteMin;
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

  // Load Yaml node (Edgelist metadata).
  YAML::Node input_node;
  input_node = YAML::LoadFile(input_path_ + "meta.yaml");
  auto edgelist_metadata = input_node["EdgelistBin"].as<EdgelistMetadata>();

  // Create Edgelist Graph.
  auto aligned_max_vid = ((edgelist_metadata.max_vid >> 6) << 6) + 64;
  auto bitmap = Bitmap(aligned_max_vid);
  auto buffer_edges = new VertexID[edgelist_metadata.num_edges * 2];
  std::ifstream input_stream(input_path_ + "edgelist.bin", std::ios::binary);
  if (!input_stream.is_open()) LOG_FATAL("Cannot open edgelist.bin");

  input_stream.read(reinterpret_cast<char*>(buffer_edges),
                    sizeof(VertexID) * edgelist_metadata.num_edges * 2);
  input_stream.close();

  // Generate vertices.
  auto num_inedges_by_vid = new VertexID[aligned_max_vid]();
  auto num_outedges_by_vid = new VertexID[aligned_max_vid]();

  auto visited = Bitmap(aligned_max_vid);
  VertexID max_vid = 0, min_vid = MAX_VERTEX_ID;

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &edgelist_metadata, &buffer_edges,
                           &num_inedges_by_vid, &num_outedges_by_vid, &visited,
                           &max_vid, &min_vid]() {
      for (VertexID j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto src = buffer_edges[j * 2];
        auto dst = buffer_edges[j * 2 + 1];
        visited.SetBit(src);
        visited.SetBit(dst);
        WriteAdd(num_inedges_by_vid + dst, (VertexID)1);
        WriteAdd(num_outedges_by_vid + src, (VertexID)1);
        WriteMax(&max_vid, src);
        WriteMax(&max_vid, dst);
        WriteMin(&min_vid, src);
        WriteMin(&min_vid, dst);
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  Vertex* buffer_csr_vertices = new Vertex[aligned_max_vid]();
  VertexID count_in_edges = 0, count_out_edges = 0;

  // malloc space for each vertex.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task =
        std::bind([i, parallelism, &aligned_max_vid, &num_inedges_by_vid,
                   &num_outedges_by_vid, &buffer_csr_vertices, &count_in_edges,
                   &count_out_edges, &visited]() {
          for (VertexID j = i; j < aligned_max_vid; j += parallelism) {
            if (!visited.GetBit(j)) continue;
            buffer_csr_vertices[j].vid = j;
            buffer_csr_vertices[j].indegree = num_inedges_by_vid[j];
            buffer_csr_vertices[j].outdegree = num_outedges_by_vid[j];
            buffer_csr_vertices[j].incoming_edges =
                new VertexID[num_inedges_by_vid[j]]();
            buffer_csr_vertices[j].outgoing_edges =
                new VertexID[num_outedges_by_vid[j]]();
            WriteAdd(&count_in_edges, num_inedges_by_vid[j]);
            WriteAdd(&count_out_edges, num_outedges_by_vid[j]);
          }
        });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete num_inedges_by_vid;
  delete num_outedges_by_vid;

  // Fill edges.
  VertexID* offset_in_edges = new VertexID[aligned_max_vid]();
  VertexID* offset_out_edges = new VertexID[aligned_max_vid]();

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &edgelist_metadata, &buffer_edges,
                           &offset_in_edges, &offset_out_edges,
                           &buffer_csr_vertices]() {
      for (VertexID j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto src = buffer_edges[j * 2];
        auto dst = buffer_edges[j * 2 + 1];
        auto offset_out = __sync_fetch_and_add(offset_out_edges + src, 1);
        auto offset_in = __sync_fetch_and_add(offset_in_edges + dst, 1);
        buffer_csr_vertices[src].outgoing_edges[offset_out] = dst;
        buffer_csr_vertices[dst].incoming_edges[offset_in] = src;
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  delete buffer_edges;
  delete offset_in_edges;
  delete offset_out_edges;

  // Construct subgraphs.
  std::vector<std::vector<Vertex>*> vertex_buckets;
  vertex_buckets.reserve(n_partitions_);

  for (VertexID i = 0; i < n_partitions_; i++) {
    vertex_buckets[i] = new std::vector<Vertex>();
    vertex_buckets[i]->reserve(aligned_max_vid / n_partitions_);
  }

  // Fill buckets.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism, n_partitions = n_partitions_]() {
      for (VertexID j = i; j < edgelist_metadata.num_vertices;
           j += parallelism) {
        auto gid = GetBucketID(buffer_csr_vertices[j].vid, n_partitions,
                               edgelist_metadata.num_vertices);
        vertex_buckets[gid]->push_back(buffer_csr_vertices[j]);
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (VertexID j = i; j < n_partitions_; j += parallelism) {
        std::sort(vertex_buckets[j]->begin(), vertex_buckets[j]->end(),
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

  graph_format_converter.WriteSubgraph(vertex_buckets, graph_metadata,
                                       store_strategy_);

  for (VertexID i = 0; i < n_partitions_; i++) delete vertex_buckets[i];
  delete[] buffer_csr_vertices;
  LOG_INFO("Finished writing the subgraphs to disk");
}

}  // namespace sics::graph::tools::partitioner
