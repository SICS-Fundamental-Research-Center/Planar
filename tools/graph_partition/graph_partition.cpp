// This file belongs to the SICS graph-systems project, a C++ library for
// exploiting parallelism graph computing. TO DO
//

#include <fstream>
#include <iostream>
#include <list>
#include <string>
#include <type_traits>

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/concurrency/UnboundedQueue.h>
#include <folly/hash/Hash.h>
#include <gflags/gflags.h>
#include <yaml-cpp/yaml.h>

#include "common/bitmap.h"
#include "common/multithreading/thread_pool.h"
#include "common/types.h"
#include "tools_common/data_structures.h"
#include "tools_common/io.h"
#include "tools_common/types.h"
#include "tools_common/yaml_config.h"
#include "util/atomic.h"
#include "util/logging.h"

using folly::ConcurrentHashMap;
using folly::UnboundedQueue;
using folly::hash::fnv64_append_byte;
using sics::graph::core::common::Bitmap;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexLabel;
using sics::graph::core::util::atomic::WriteAdd;
using std::filesystem::create_directory;
using std::filesystem::exists;
using namespace tools;

enum Partitioner {
  kEdgeCut,  // default
  kVertexCut,
  kHybridCut,
  kUndefinedPartitioner
};

inline Partitioner String2EnumPartitioner(const std::string& s) {
  if (s == "edgecut")
    return kEdgeCut;
  else if (s == "vertexcut")
    return kVertexCut;
  else if (s == "hybridcut")
    return kHybridCut;
  else
    return kUndefinedPartitioner;
};

DEFINE_string(partitioner, "", "partitioner type.");
DEFINE_string(i, "", "input path.");
DEFINE_string(o, "", "output path.");
DEFINE_uint64(n_partitions, 1, "the number of partitions");
DEFINE_string(store_strategy, "unconstrained",
              "graph-systems adopted three strategies to store edges: "
              "kUnconstrained, incoming, and outgoing.");
DEFINE_string(convert_mode, "", "Conversion mode");
DEFINE_string(sep, "", "separator to split csv file.");
DEFINE_bool(read_head, false, "whether to read header of csv.");

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
bool EdgeCut(const std::string& input_path, const std::string& output_path,
             const Partitioner partitioner, const size_t n_partitions,
             StoreStrategy store_strategy) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();

  // Load Yaml node (Edgelist metadata).
  YAML::Node input_node;
  input_node = YAML::LoadFile(input_path + "meta.yaml");
  LOG_INFO(input_path + "meta.yaml");
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
  auto num_inedges_by_vid = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  auto num_outedges_by_vid = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  auto visited = Bitmap(aligned_max_vid);
  visited.Clear();
  memset(num_inedges_by_vid, 0, sizeof(size_t) * aligned_max_vid);
  memset(num_outedges_by_vid, 0, sizeof(size_t) * aligned_max_vid);

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &edgelist_metadata, &buffer_edges,
                           &num_inedges_by_vid, &num_outedges_by_vid,
                           &visited]() {
      for (size_t j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto src = buffer_edges[j * 2];
        auto dst = buffer_edges[j * 2 + 1];
        visited.SetBit(src);
        visited.SetBit(dst);
        WriteAdd(num_inedges_by_vid + dst, (size_t)1);
        WriteAdd(num_outedges_by_vid + src, (size_t)1);
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  for (size_t i = 0; i < edgelist_metadata.num_vertices; i++) {
    if (!visited.GetBit(i)) {
      printf("Vertex %lu is isolated\n", i);
      continue;
    }
    LOG_INFO(i, " ", num_inedges_by_vid[i], " ", num_outedges_by_vid[i]);
  }

  TMPCSRVertex* buffer_csr_vertices =
      (TMPCSRVertex*)malloc(sizeof(TMPCSRVertex) * aligned_max_vid);
  size_t count_in_edges = 0, count_out_edges = 0;

  // malloc space for each vertex.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &aligned_max_vid, &buffer_edges,
                           &num_inedges_by_vid, &num_outedges_by_vid,
                           &buffer_csr_vertices, &count_in_edges,
                           &count_out_edges, &visited]() {
      for (size_t j = i; j < aligned_max_vid; j += parallelism) {
        if (!visited.GetBit(j)) continue;
        auto u = TMPCSRVertex();
        u.vid = j;
        u.indegree = num_inedges_by_vid[j];
        u.outdegree = num_outedges_by_vid[j];
        u.in_edges = (VertexID*)malloc(sizeof(VertexID) * u.indegree);
        u.out_edges = (VertexID*)malloc(sizeof(VertexID) * u.outdegree);
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
  size_t* offset_in_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  size_t* offset_out_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  memset(offset_in_edges, 0, sizeof(size_t) * aligned_max_vid);
  memset(offset_out_edges, 0, sizeof(size_t) * aligned_max_vid);
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &edgelist_metadata, &buffer_edges,
                           &offset_in_edges, &offset_out_edges,
                           &buffer_csr_vertices]() {
      for (size_t j = i; j < edgelist_metadata.num_edges; j += parallelism) {
        auto src = buffer_edges[j * 2];
        auto dst = buffer_edges[j * 2 + 1];
        auto offset_out = __sync_fetch_and_add(offset_out_edges + src, 1);
        auto offset_in = __sync_fetch_and_add(offset_in_edges + dst, 1);
        buffer_csr_vertices[src].out_edges[offset_out] = dst;
        buffer_csr_vertices[dst].in_edges[offset_in] = src;
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  delete buffer_edges;

  // Construct subgraphs.
  std::vector<ConcurrentHashMap<VertexID, TMPCSRVertex>*> subgraph_vec;

  for (size_t i = 0; i < n_partitions; i++)
    subgraph_vec.push_back(
        new ConcurrentHashMap<VertexID, TMPCSRVertex>(aligned_max_vid));

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &edgelist_metadata,
                           &buffer_csr_vertices, &subgraph_vec,
                           &n_partitions]() {
      for (size_t j = i; j < edgelist_metadata.num_vertices; j += parallelism) {
        auto gid =
            fnv64_append_byte(buffer_csr_vertices[j].vid, 1) % n_partitions;
        subgraph_vec.at(gid)->insert(
            std::make_pair(buffer_csr_vertices[j].vid, buffer_csr_vertices[j]));
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete buffer_csr_vertices;

  // Write the subgraphs to disk
  CSRIOAdapter csr_io_adapter(output_path);
  csr_io_adapter.WriteSubgraph(subgraph_vec, store_strategy);

  input_stream.close();
  return 0;
}

bool VertexCut(const std::string& input_path, const std::string& output_path,
               const Partitioner partitioner, const size_t n_partitions,
               StoreStrategy store_strategy) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();

  // Load Yaml node (Edgelist metadata).
  YAML::Node input_node;
  input_node = YAML::LoadFile(input_path + "meta.yaml");
  LOG_INFO(input_path + "meta.yaml");
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

  // TODO (hsiaoko): vertex cut.

  return 0;
}

int main(int argc, char** argv) {
  gflags::SetUsageMessage(
      "\n USAGE: graph-convert --convert_mode=[options] -i <input file path> "
      "-o <output file path> --sep=[separator] \n"
      " General options:\n"
      "\t edgelistcsv2edgelistbin:  - Convert edge list txt to binary edge "
      "list\n"
      "\t edgelistcsv2csrbin:   - Convert edge list of txt format to binary "
      "csr\n"
      "\t edgelistbin2csrbin:   - Convert edge list of bin format to binary "
      "csr\n");

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_i == "" || FLAGS_o == "") {
    LOG_ERROR("Input (output) path is empty.");
    return -1;
  }

  switch (String2EnumPartitioner(FLAGS_partitioner)) {
    case kVertexCut:
      // TODO (hsaioko): Add VertexCut partitioner.
      break;
    case kEdgeCut:
      EdgeCut(FLAGS_i, FLAGS_o, String2EnumPartitioner(FLAGS_partitioner),
              FLAGS_n_partitions,
              tools::String2EnumStoreStrategy(FLAGS_store_strategy));
      break;
    case kHybridCut:
      // TODO (hsiaoko): Add HybridCut partitioner.
      break;
    default:
      LOG_INFO("Error graph partitioner.");
      break;
  }

  gflags::ShutDownCommandLineFlags();
  return 0;
}