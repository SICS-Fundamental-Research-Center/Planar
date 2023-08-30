#include "tools/graph_partitioner/partitioner/hash_based_vertexcut.h"

#include <filesystem>
#include <string>

#include <folly/concurrency/ConcurrentHashMap.h>
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

VertexID HashBasedVertexCutPartitioner::GetBucketID(VertexID vid,
                                                    VertexID n_bucket) const {
  return fnv64_append_byte(vid, 3) % n_bucket;
}

void HashBasedVertexCutPartitioner::RunPartitioner() {
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
  auto buffer_edges = new VertexID[edgelist_metadata.num_edges * 2]();
  std::ifstream input_stream(input_path_ + "edgelist.bin", std::ios::binary);
  if (!input_stream.is_open()) LOG_FATAL("Cannot open edgelist.bin");
  input_stream.read(reinterpret_cast<char*>(buffer_edges),
                    sizeof(VertexID) * edgelist_metadata.num_edges * 2);

  // Precompute the size of each edge bucket.
  auto size_per_bucket = new VertexID[n_partitions_]();
  auto max_vid_per_bucket = new VertexID[n_partitions_]();
  auto min_vid_per_bucket = new VertexID[n_partitions_]();

  VertexID max_vid = 0, min_vid = MAX_VERTEX_ID;
  for (VertexID i = 0; i < n_partitions_; i++)
    min_vid_per_bucket[i] = MAX_VERTEX_ID;

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (VertexID j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto src = buffer_edges[j * 2];
        auto dst = buffer_edges[j * 2 + 1];
        VertexID bid;
        switch (store_strategy_) {
          case kOutgoingOnly:
            bid = GetBucketID(src, n_partitions_);
            break;
          case kIncomingOnly:
            bid = GetBucketID(dst, n_partitions_);
            break;
          case kUnconstrained:
            bid = GetBucketID(src, n_partitions_);
            break;
          default:
            LOG_FATAL("Undefined store strategy.");
        }
        WriteAdd(size_per_bucket + bid, (VertexID)1);
        WriteMax(max_vid_per_bucket + bid, src);
        WriteMax(max_vid_per_bucket + bid, dst);
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

  auto edge_bucket = new VertexID*[n_partitions_]();
  for (size_t i = 0; i < n_partitions_; i++)
    edge_bucket[i] = new VertexID[size_per_bucket[i] * 2]();

  auto bitmap_vec = new std::vector<Bitmap*>();
  for (size_t i = 0; i < n_partitions_; i++) {
    auto bitmap = new Bitmap(aligned_max_vid);
    bitmap->Clear();
    bitmap_vec->push_back(bitmap);
  }

  auto buckets_offset = new VertexID[n_partitions_]();

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (VertexID j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto src = buffer_edges[j * 2];
        auto dst = buffer_edges[j * 2 + 1];
        VertexID bid;
        switch (store_strategy_) {
          case kOutgoingOnly:
            bid = GetBucketID(src, n_partitions_);
            break;
          case kIncomingOnly:
            bid = GetBucketID(dst, n_partitions_);
            break;
          case kUnconstrained:
            bid = GetBucketID(src, n_partitions_);
            break;
          default:
            LOG_FATAL("Undefined store strategy.");
        }
        auto offset = __sync_fetch_and_add(buckets_offset + bid, 1);
        edge_bucket[bid][offset * 2] = src;
        edge_bucket[bid][offset * 2 + 1] = dst;
        bitmap_vec->at(bid)->SetBit(src);
        bitmap_vec->at(bid)->SetBit(dst);
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  delete buffer_edges;

  std::vector<EdgelistMetadata> edgelist_metadata_vec;
  for (size_t i = 0; i < n_partitions_; i++) {
    edgelist_metadata_vec.push_back({bitmap_vec->at(i)->Count(),
                                     size_per_bucket[i],
                                     max_vid_per_bucket[i]});
  }

  // Write the subgraphs to disk
  GraphFormatConverter graph_format_converter(output_path_);
  GraphMetadata graph_metadata;
  graph_metadata.set_num_vertices(edgelist_metadata.num_vertices);
  graph_metadata.set_num_edges(edgelist_metadata.num_edges);
  graph_metadata.set_num_subgraphs(n_partitions_);
  graph_metadata.set_max_vid(max_vid);
  graph_metadata.set_min_vid(min_vid);

  graph_format_converter.WriteSubgraph(edge_bucket, graph_metadata,
                                       edgelist_metadata_vec, store_strategy_);

  input_stream.close();
  LOG_INFO("Finished writing the subgraphs to disk");
}

}  // namespace sics::graph::tools::partitioner
