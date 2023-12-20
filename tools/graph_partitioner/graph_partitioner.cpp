// This file belongs to the SICS graph-systems project, a C++ library for
// exploiting parallelism graph computing. TODO (hsiaoko): add description
#include <gflags/gflags.h>

#include "core/common/multithreading/thread_pool.h"
#include "core/util/logging.h"
#include "tools/common/yaml_config.h"
#include "tools/graph_partitioner/partitioner/csr_based_planar_vertexcut.h"
#include "tools/graph_partitioner/partitioner/hash_based_edgecut.h"
#include "tools/graph_partitioner/partitioner/hash_based_vertexcut.h"

using sics::graph::tools::common::StoreStrategy2Enum;
using EdgeCutPartitioner =
    sics::graph::tools::partitioner::HashBasedEdgeCutPartitioner;
using VertexCutPartitioner =
    sics::graph::tools::partitioner::HashBasedVertexCutPartitioner;
using PlanarVertexCutPartitioner =
    sics::graph::tools::partitioner::CSRBasedPlanarVertexCutPartitioner;

enum Partitioner {
  kHashEdgeCut,  // default
  kHashVertexCut,
  kHybridCut,
  kPlanarVertexCut,
  kUndefinedPartitioner
};

Partitioner Partitioner2Enum(const std::string& s) {
  if (s == "hashedgecut")
    return kHashEdgeCut;
  else if (s == "hashvertexcut")
    return kHashVertexCut;
  else if (s == "hybridcut")
    return kHybridCut;
  else if (s == "planarvertexcut")
    return kPlanarVertexCut;
  else
    LOG_FATAL("Unknown partitioner type: ", s.c_str());
  return kUndefinedPartitioner;
};

DEFINE_string(partitioner, "", "partitioner type.");
DEFINE_string(i, "", "input path.");
DEFINE_string(o, "", "output path.");
DEFINE_uint64(n_partitions, 1, "the number of partitions");
DEFINE_string(store_strategy, "unconstrained",
              "graph-systems adopted three strategies to store edges: "
              "kUnconstrained, incoming, and outgoing.");
DEFINE_bool(biggraph, false, "for big graphs.");

int main(int argc, char** argv) {
  gflags::SetUsageMessage(
      "\n USAGE: graph-partitioner -partitioner [options] -i [input "
      "path] "
      "-o [output path] -n_partitions [number of partitions]\n"
      " General options:\n"
      "\t hashedgecut: - Using hash-based edge cut partitioner "
      "\n"
      "\t hashvertexcut: - Using hash-based vertex cut partitioner"
      "\n"
      "\t hybridcut:   - Using hybrid cut partitioner "
      "\n");

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_i == "" || FLAGS_o == "")
    LOG_FATAL("Input (output) path is empty.");

  switch (Partitioner2Enum(FLAGS_partitioner)) {
    case kHashVertexCut: {
      VertexCutPartitioner vertexcut_partitioner(
          FLAGS_i, FLAGS_o, StoreStrategy2Enum(FLAGS_store_strategy),
          FLAGS_n_partitions);
      vertexcut_partitioner.RunPartitioner();
      break;
    }
    case kHashEdgeCut: {
      EdgeCutPartitioner edgecut_partitioner(
          FLAGS_i, FLAGS_o, StoreStrategy2Enum(FLAGS_store_strategy),
          FLAGS_n_partitions);
      edgecut_partitioner.RunPartitioner();
      break;
    }
    case kPlanarVertexCut: {
      PlanarVertexCutPartitioner planar_vertexcut_partitioner(
          FLAGS_i, FLAGS_o, StoreStrategy2Enum(FLAGS_store_strategy),
          FLAGS_n_partitions);
      planar_vertexcut_partitioner.RunPartitioner(FLAGS_biggraph);
      break;
    }
    case kHybridCut:
      // TODO (hsaioko): Add HyrbidCut partitioner.
      break;
    default:
      LOG_FATAL("Error graph partitioner.");
  }

  gflags::ShutDownCommandLineFlags();
  return 0;
}