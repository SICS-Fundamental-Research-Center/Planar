// This file belongs to the SICS graph-systems project, a C++ library for
// exploiting parallelism graph computing. TODO (hsiaoko): add description
//

#include <gflags/gflags.h>

#include "core/common/multithreading/thread_pool.h"
#include "core/util/logging.h"
#include "tools/common/yaml_config.h"
#include "tools/graph_partitioner/partitioner/edgecut.h"
#include "tools/graph_partitioner/partitioner/vertexcut.h"

using sics::graph::tools::common::StoreStrategy2Enum;
using sics::graph::tools::partitioner::EdgeCutPartitioner;
using sics::graph::tools::partitioner::VertexCutPartitioner;

enum Partitioner {
  kEdgeCut,  // default
  kVertexCut,
  kHybridCut,
  kUndefinedPartitioner
};

Partitioner Partitioner2Enum(const std::string& s) {
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
  if (FLAGS_i == "" || FLAGS_o == "")
    LOG_FATAL("Input (output) path is empty.");

  switch (Partitioner2Enum(FLAGS_partitioner)) {
    case kVertexCut: {
      VertexCutPartitioner vertexcut_partitioner(
          FLAGS_i, FLAGS_o, StoreStrategy2Enum(FLAGS_store_strategy),
          FLAGS_n_partitions);
      vertexcut_partitioner.RunPartitioner();
      break;
    }
    case kEdgeCut: {
      EdgeCutPartitioner edgecut_partitioner(
          FLAGS_i, FLAGS_o, StoreStrategy2Enum(FLAGS_store_strategy),
          FLAGS_n_partitions);
      edgecut_partitioner.RunPartitioner();
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