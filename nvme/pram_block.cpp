#include <gflags/gflags.h>

#include <fstream>

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph_metadata.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_string(out, "/", "output dir for block files");
DEFINE_uint32(step_v, 500000, "vertex step for block");
DEFINE_uint32(p, 8, "parallelism");

using namespace sics::graph;
using sics::graph::core::common::VertexID;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string root_path = FLAGS_i;
  auto step = FLAGS_step_v;
  auto parallelism = FLAGS_p;
  auto out_dir = FLAGS_out;

  // read graph of one subgraph
  core::data_structures::GraphMetadata graph_metadata(root_path);
  if (graph_metadata.get_num_subgraphs() != 1) {
    LOG_FATAL("num subgraphs: ", graph_metadata.get_num_subgraphs());
  }

  // separate the graph into blocks
  // use no local index, only use block ID and vertex ID
  core::common::ThreadPool thread_pool(parallelism);
  core::common::TaskPackage tasks;

  VertexID bid = 0, eid = 0 + step;
  bool flag = true;
  int file_id = 0;
  while (flag) {
    if (eid > graph_metadata.get_num_vertices()) {
      eid = graph_metadata.get_num_vertices();
      flag = false;
    }
    auto task = [&, bid, eid, out_dir, file_id]() {

      // write back to disk

    };
    tasks.push_back(task);
    bid = eid;
    eid += step;
    file_id++;
  }
  thread_pool.SubmitSync(tasks);
}