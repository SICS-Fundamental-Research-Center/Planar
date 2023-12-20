#include "tools/graph_partitioner/partitioner/hash_based_vertexcut.h"

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/hash/Hash.h>

#include <filesystem>
#include <string>

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

VertexID HashBasedVertexCutPartitioner::GetBucketID(VertexID vid,
                                                    VertexID n_bucket) const {
  return fnv64_append_byte(vid, 3) % n_bucket;
}

void HashBasedVertexCutPartitioner::RunPartitioner() {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();
  task_package.reserve(parallelism);

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
  Edges edges(edgelist_metadata, reinterpret_cast<Edge*>(buffer_edges));
  // edges.SortBySrc();

  // Precompute the size of each edge bucket.
  auto size_per_bucket = new EdgeIndex[n_partitions_]();
  auto max_vid_per_bucket = new VertexID[n_partitions_]();
  auto min_vid_per_bucket = new VertexID[n_partitions_]();

  VertexID max_vid = 0, min_vid = MAX_VERTEX_ID;
  for (VertexID i = 0; i < n_partitions_; i++)
    min_vid_per_bucket[i] = MAX_VERTEX_ID;

  std::vector<Bitmap> bitmap_vec;
  bitmap_vec.resize(n_partitions_, aligned_max_vid);

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (EdgeIndex j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto e = edges.get_edge_by_index(j);
        VertexID bid;
        switch (store_strategy_) {
          case kOutgoingOnly:
            bid = GetBucketID(e.src, n_partitions_);
            break;
          case kIncomingOnly:
            bid = GetBucketID(e.dst, n_partitions_);
            break;
          case kUnconstrained:
            bid = GetBucketID(e.src, n_partitions_);
            break;
          default:
            LOG_FATAL("Undefined store strategy.");
        }
        bitmap_vec.at(bid).SetBit(e.src);
        bitmap_vec.at(bid).SetBit(e.dst);
        WriteAdd(size_per_bucket + bid, (EdgeIndex)1);
        WriteMax(max_vid_per_bucket + bid, e.src);
        WriteMax(max_vid_per_bucket + bid, e.dst);
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

  std::vector<EdgelistMetadata> edgelist_metadata_vec;
  std::vector<Edges> edge_buckets;

  for (GraphID i = 0; i < n_partitions_; i++) {
    EdgelistMetadata edgelist_metadata = {(uint32_t)(bitmap_vec.at(i).Count()),
                                          size_per_bucket[i],
                                          max_vid_per_bucket[i]};
    edgelist_metadata_vec.push_back(edgelist_metadata);
    edge_buckets.emplace_back(Edges(edgelist_metadata));
  }
  delete[] max_vid_per_bucket;
  delete[] min_vid_per_bucket;
  delete[] size_per_bucket;

  auto bucket_offset = new EdgeIndex[n_partitions_]();

  std::mutex mtx;
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (EdgeIndex j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto e = edges.get_edge_by_index(j);
        VertexID bid;
        switch (store_strategy_) {
          case kOutgoingOnly:
            bid = GetBucketID(e.src, n_partitions_);
            break;
          case kIncomingOnly:
            bid = GetBucketID(e.dst, n_partitions_);
            break;
          case kUnconstrained:
            bid = GetBucketID(e.src, n_partitions_);
            break;
          default:
            LOG_FATAL("Undefined store strategy.");
        }
        EdgeIndex offset = __sync_fetch_and_add(&bucket_offset[bid], 1);
        auto edges_ptr = edge_buckets[bid].get_base_ptr();
        edges_ptr[offset] = e;
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete[] bucket_offset;

  // Write the subgraphs to disk
  GraphFormatConverter graph_format_converter(output_path_);
  GraphMetadata graph_metadata;
  graph_metadata.set_num_vertices(edgelist_metadata.num_vertices);
  graph_metadata.set_num_edges(edgelist_metadata.num_edges);
  graph_metadata.set_num_subgraphs(n_partitions_);
  graph_metadata.set_max_vid(max_vid);
  graph_metadata.set_min_vid(min_vid);

  LOG_INFO("Writing the subgraphs to disk");
  graph_format_converter.WriteSubgraph(edge_buckets, graph_metadata,
                                       store_strategy_);
  input_stream.close();
  LOG_INFO("Finished writing the subgraphs to disk");
}

}  // namespace sics::graph::tools::partitioner
