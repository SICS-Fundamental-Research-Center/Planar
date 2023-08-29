// This file belongs to the SICS graph-systems project, a C++ library for
// exploiting parallelism graph computing. TODO (hsiaoko): add description
//

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/concurrency/UnboundedQueue.h>
#include <folly/hash/Hash.h>
#include <gflags/gflags.h>
#include <yaml-cpp/yaml.h>

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/common/types.h"
#include "core/util/atomic.h"
#include "core/util/logging.h"
#include "tools/common/data_structures.h"
#include "tools/common/io.h"
#include "tools/common/types.h"
#include "tools/common/yaml_config.h"

using folly::ConcurrentHashMap;
using folly::UnboundedQueue;
using folly::hash::fnv64_append_byte;
using sics::graph::core::common::Bitmap;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexLabel;
using sics::graph::core::data_structures::GraphMetadata;
using Vertex = sics::graph::core::data_structures::graph::ImmutableCSRVertex;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::atomic::WriteMax;
using sics::graph::core::util::atomic::WriteMin;
using std::filesystem::create_directory;
using std::filesystem::exists;
using namespace sics::graph::tools::common;

enum Partitioner {
  kEdgeCut,  // default
  kVertexCut,
  kHybridCut,
  kUndefinedPartitioner
};

inline Partitioner Partitioner2Enum(const std::string& s) {
  if (s == "edgecut")
    return kEdgeCut;
  else if (s == "vertexcut")
    return kVertexCut;
  else if (s == "hybridcut")
    return kHybridCut;
  else
    LOG_ERROR("Unknown partitioner type: %s", s.c_str());
  return kUndefinedPartitioner;
};

DEFINE_string(partitioner, "", "partitioner type.");
DEFINE_string(i, "", "input path.");
DEFINE_string(o, "", "output path.");
DEFINE_uint64(n_partitions, 1, "the number of partitions");
DEFINE_string(store_strategy, "unconstrained",
              "graph-systems adopted three strategies to store edges: "
              "kUnconstrained, incoming, and outgoing.");

// @DESCRIPTION With edgecut partitioner, each vertex is assigned to a
// fragment. In a fragment, inner vertices are those vertices assigned to it,
// and the outer vertices are the remaining vertices adjacent to some of inner
// vertices. The load strategy defines how to store the adjacency between inner
// and outer vertices.
//
// For example, a graph
// G = {V, E}
// V = {v0, v1, v2, v3, v4}
// E = {(v0, v2), (v0, v3), (v1, v0), (v3, v1), (v3, v4), (v4, v1), (v4, v2)}
// is split into F0 that consists of V_F0: {v0, v1, v2}, E_F0: {(v0,
// v2), (v0, v3), (v1, v0)} and F1 that consists of V_F1: {v3, v4}, E_F1: {(v3,
// v1), (v3, v4), (v4, v1), (v4, v2)}
// @PARAMETERS
// partitioner: The partitioner to use.
// n_partitions: The number of partitions to use.
// store_strategy: The strategy to use to store edges.
bool EdgeCut(const std::string& input_path, const std::string& output_path,
             const Partitioner partitioner, const VertexID n_partitions,
             StoreStrategy store_strategy) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();

  // Load Yaml node (Edgelist metadata).
  YAML::Node input_node;
  input_node = YAML::LoadFile(input_path + "meta.yaml");
  auto edgelist_metadata = input_node["EdgelistBin"].as<EdgelistMetadata>();

  // Create Edgelist Graph.
  auto aligned_max_vid = ((edgelist_metadata.max_vid >> 6) << 6) + 64;
  auto bitmap = Bitmap(aligned_max_vid);
  auto buffer_edges =
      (VertexID*)malloc(sizeof(VertexID) * edgelist_metadata.num_edges * 2);
  std::ifstream input_stream(input_path + "edgelist.bin", std::ios::binary);
  if (!input_stream.is_open()) {
    LOG_ERROR("Cannot open edgelist.bin");
    return -1;
  }
  input_stream.read(reinterpret_cast<char*>(buffer_edges),
                    sizeof(VertexID) * edgelist_metadata.num_edges * 2);

  // Generate vertices.
  auto num_inedges_by_vid =
      (VertexID*)malloc(sizeof(VertexID) * aligned_max_vid);
  auto num_outedges_by_vid =
      (VertexID*)malloc(sizeof(VertexID) * aligned_max_vid);
  auto visited = Bitmap(aligned_max_vid);
  visited.Clear();
  memset(num_inedges_by_vid, 0, sizeof(VertexID) * aligned_max_vid);
  memset(num_outedges_by_vid, 0, sizeof(VertexID) * aligned_max_vid);
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
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  Vertex* buffer_csr_vertices =
      (Vertex*)malloc(sizeof(Vertex) * aligned_max_vid);
  VertexID count_in_edges = 0, count_out_edges = 0;

  // malloc space for each vertex.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &aligned_max_vid, &buffer_edges,
                           &num_inedges_by_vid, &num_outedges_by_vid,
                           &buffer_csr_vertices, &count_in_edges,
                           &count_out_edges, &visited]() {
      for (VertexID j = i; j < aligned_max_vid; j += parallelism) {
        if (!visited.GetBit(j)) continue;
        auto u = Vertex();
        u.vid = j;
        u.indegree = num_inedges_by_vid[j];
        u.outdegree = num_outedges_by_vid[j];
        u.incoming_edges = (VertexID*)malloc(sizeof(VertexID) * u.indegree);
        u.outgoing_edges = (VertexID*)malloc(sizeof(VertexID) * u.outdegree);
        WriteAdd(&count_in_edges, u.indegree);
        WriteAdd(&count_out_edges, u.outdegree);
        buffer_csr_vertices[j] = u;
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete num_inedges_by_vid;

  // Fill edges.
  VertexID* offset_in_edges =
      (VertexID*)malloc(sizeof(VertexID) * aligned_max_vid);
  VertexID* offset_out_edges =
      (VertexID*)malloc(sizeof(VertexID) * aligned_max_vid);
  memset(offset_in_edges, 0, sizeof(VertexID) * aligned_max_vid);
  memset(offset_out_edges, 0, sizeof(VertexID) * aligned_max_vid);
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
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  delete buffer_edges;
  delete offset_in_edges;
  delete offset_out_edges;

  // Construct subgraphs.
  std::vector<ConcurrentHashMap<VertexID, Vertex>*> subgraph_vec;

  for (VertexID i = 0; i < n_partitions; i++)
    subgraph_vec.push_back(
        new ConcurrentHashMap<VertexID, Vertex>(aligned_max_vid));

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task =
        std::bind([i, parallelism, &edgelist_metadata, &buffer_csr_vertices,
                   &subgraph_vec, &n_partitions]() {
          for (VertexID j = i; j < edgelist_metadata.num_vertices;
               j += parallelism) {
            auto gid =
                fnv64_append_byte(buffer_csr_vertices[j].vid, 1) % n_partitions;
            subgraph_vec.at(gid)->insert(std::make_pair(
                buffer_csr_vertices[j].vid, buffer_csr_vertices[j]));
          }
          return;
        });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  delete buffer_csr_vertices;

  // Write the subgraphs to disk
  GraphFormatConverter graph_format_converter(output_path);
  GraphMetadata graph_metadata;
  graph_metadata.set_num_vertices(edgelist_metadata.num_vertices);
  graph_metadata.set_num_edges(edgelist_metadata.num_edges);
  graph_metadata.set_num_subgraphs(n_partitions);
  graph_metadata.set_max_vid(max_vid);
  graph_metadata.set_min_vid(min_vid);

  graph_format_converter.WriteSubgraph(subgraph_vec,
                                       graph_metadata,
                                       store_strategy);
  input_stream.close();
  LOG_INFO("Finished writing the subgraphs to disk");
  return 0;
}

// @DESCRIPTION A vertex-cut partitioning divides edges of a graph into equal
// size clusters. f vertex-cuts are used. A vertex-cut partitioning divides
// edges of a graph into equal size clusters. The vertices that hold the
// endpoints of an edge are also placed in the same cluster as the edge itself.
// However, the vertices are not unique across clusters and might have to be
// replicated, due to the distribution of their edges across different clusters.

// For example, a graph
// G = {V, E}
// V = {v0, v1, v2, v3, v4}
// E = {(v0, v2), (v0, v3), (v1, v0), (v3, v1), (v3, v4), (v4, v1), (v4, v2)}
// is split into F0 that consists of V_F0: {v0, v1, v2, v3}, E_F0: {(v0,
// v2), (v0, v3), (v1, v0)} and F1 that consists of V_F1: {v1, v2, v3, v4},
// E_F1: {(v3, v1), (v3, v4), (v4, v1), (v4, v2)}
// @PARAMETERS
// input_path: Path to the input graph file.
// output_path: Path to the output partition file.
// partitioner: Partitioner to use [edgecut, vertexcut, hybridcut].
// n_partitions: Number of partitions to use.
// store_strategy: StoreStrategy to use [incoming_only, outgoing_only,
// unconstrained], corresponding to store incoming edges only, outgoing edges
// only, and both two respectively.
bool VertexCut(const std::string& input_path, const std::string& output_path,
               const Partitioner partitioner, const size_t n_partitions,
               StoreStrategy store_strategy) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();

  // Load Yaml node (Edgelist metadata).
  YAML::Node input_node;
  input_node = YAML::LoadFile(input_path + "meta.yaml");
  auto edgelist_metadata = input_node["EdgelistBin"].as<EdgelistMetadata>();

  // Create Edgelist Graph.
  auto aligned_max_vid = ((edgelist_metadata.max_vid >> 6) << 6) + 64;
  auto bitmap = Bitmap(aligned_max_vid);
  auto buffer_edges =
      (VertexID*)malloc(sizeof(VertexID) * edgelist_metadata.num_edges * 2);
  std::ifstream input_stream(input_path + "edgelist.bin", std::ios::binary);
  if (!input_stream.is_open()) {
    LOG_ERROR("Cannot open edgelist.bin");
    return -1;
  }
  input_stream.read(reinterpret_cast<char*>(buffer_edges),
                    sizeof(VertexID) * edgelist_metadata.num_edges * 2);

  // Precompute the size of each edge bucket.
  auto size_per_bucket = (VertexID*)malloc(sizeof(VertexID) * n_partitions);
  memset(size_per_bucket, 0, sizeof(VertexID) * n_partitions);
  auto max_vid_per_bucket = (VertexID*)malloc(sizeof(VertexID) * n_partitions);
  memset(max_vid_per_bucket, 0, sizeof(VertexID) * n_partitions);
  auto min_vid_per_bucket = (VertexID*)malloc(sizeof(VertexID) * n_partitions);
  VertexID max_vid = 0, min_vid = MAX_VERTEX_ID;
  for (VertexID i = 0; i < n_partitions; i++)
    min_vid_per_bucket[i] = MAX_VERTEX_ID;

  memset(min_vid_per_bucket, 0, sizeof(VertexID) * n_partitions);
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &edgelist_metadata, &buffer_edges,
                           &size_per_bucket, &max_vid_per_bucket, &max_vid,
                           &min_vid, &n_partitions, &store_strategy]() {
      for (VertexID j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto src = buffer_edges[j * 2];
        auto dst = buffer_edges[j * 2 + 1];
        VertexID bid;
        switch (store_strategy) {
          case kOutgoingOnly:
            bid = fnv64_append_byte(src, 3) % n_partitions;
            break;
          case kIncomingOnly:
            bid = fnv64_append_byte(dst, 3) % n_partitions;
            break;
          case kUnconstrained:
            bid = fnv64_append_byte(src, 3) % n_partitions;
            break;
          default:
            bid = fnv64_append_byte(src, 3) % n_partitions;
            break;
        }
        WriteAdd(size_per_bucket + bid, (VertexID)1);
        WriteMax(max_vid_per_bucket + bid, src);
        WriteMax(max_vid_per_bucket + bid, dst);
        WriteMax(&max_vid, src);
        WriteMax(&max_vid, dst);
        WriteMin(&min_vid, src);
        WriteMin(&min_vid, dst);
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  auto edge_bucket = (VertexID**)malloc(sizeof(VertexID*) * n_partitions);
  for (size_t i = 0; i < n_partitions; i++) {
    edge_bucket[i] =
        (VertexID*)malloc(sizeof(VertexID) * size_per_bucket[i] * 2);
  }

  auto bitmap_vec = new std::vector<Bitmap*>();
  for (size_t i = 0; i < n_partitions; i++) {
    auto bitmap = new Bitmap(aligned_max_vid);
    bitmap->Clear();
    bitmap_vec->push_back(bitmap);
  }

  auto buckets_offset = (VertexID*)malloc(sizeof(VertexID) * n_partitions);
  memset(buckets_offset, 0, sizeof(VertexID) * n_partitions);

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &edgelist_metadata, &buffer_edges,
                           &size_per_bucket, &n_partitions, &edge_bucket,
                           &buckets_offset, &bitmap_vec, &store_strategy]() {
      for (VertexID j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto src = buffer_edges[j * 2];
        auto dst = buffer_edges[j * 2 + 1];
        VertexID bid;
        switch (store_strategy) {
          case kOutgoingOnly:
            bid = fnv64_append_byte(src, 3) % n_partitions;
            break;
          case kIncomingOnly:
            bid = fnv64_append_byte(dst, 3) % n_partitions;
            break;
          case kUnconstrained:
            bid = fnv64_append_byte(src, 3) % n_partitions;
            break;
          default:
            bid = fnv64_append_byte(src, 3) % n_partitions;
            break;
        }
        auto offset = __sync_fetch_and_add(buckets_offset + bid, 1);
        edge_bucket[bid][offset * 2] = src;
        edge_bucket[bid][offset * 2 + 1] = dst;
        bitmap_vec->at(bid)->SetBit(src);
        bitmap_vec->at(bid)->SetBit(dst);
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  delete buffer_edges;

  std::vector<EdgelistMetadata> edgelist_metadata_vec;
  for (size_t i = 0; i < n_partitions; i++) {
    edgelist_metadata_vec.push_back({bitmap_vec->at(i)->Count(),
                                     size_per_bucket[i],
                                     max_vid_per_bucket[i]});
  }

  // Write the subgraphs to disk
  GraphFormatConverter graph_format_converter(output_path);
  GraphMetadata graph_metadata;
  graph_metadata.set_num_vertices(edgelist_metadata.num_vertices);
  graph_metadata.set_num_edges(edgelist_metadata.num_edges);
  graph_metadata.set_num_subgraphs(n_partitions);
  graph_metadata.set_max_vid(max_vid);
  graph_metadata.set_min_vid(min_vid);

  graph_format_converter.WriteSubgraph(edge_bucket,
                                       graph_metadata,
                                       edgelist_metadata_vec,
                                       store_strategy);

  input_stream.close();
  LOG_INFO("Finished writing the subgraphs to disk");
  return 0;
}

int main(int argc, char** argv) {
  gflags::SetUsageMessage(
      "\n USAGE: graph-partitioner --partitioner=[options] -i <input file "
      "path> "
      "-o <output file path> --n_partitions \n"
      " General options:\n"
      "\t : edgecut: - Using edge cut partitioner "
      "\n"
      "\t vertexcut: - Using vertex cut partitioner"
      "\n"
      "\t hybridcut:   - Using hybrid cut partitioner "
      "\n");

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_i == "" || FLAGS_o == "") {
    LOG_ERROR("Input (output) path is empty.");
    return -1;
  }

  switch (Partitioner2Enum(FLAGS_partitioner)) {
    case kVertexCut:
      VertexCut(FLAGS_i, FLAGS_o, Partitioner2Enum(FLAGS_partitioner),
                FLAGS_n_partitions, StoreStrategy2Enum(FLAGS_store_strategy));
      break;
    case kEdgeCut:
      EdgeCut(FLAGS_i, FLAGS_o, Partitioner2Enum(FLAGS_partitioner),
              FLAGS_n_partitions, StoreStrategy2Enum(FLAGS_store_strategy));
      break;
    case kHybridCut:
      // TODO (hsaioko): Add HyrbidCut partitioner.
      break;
    default:
      LOG_INFO("Error graph partitioner.");
      break;
  }

  gflags::ShutDownCommandLineFlags();
  return 0;
}