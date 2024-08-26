#include <gflags/gflags.h>

#include <filesystem>
#include <fstream>

#include "core/common/bitmap.h"
#include "core/data_structures/graph_metadata.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_string(o, "/out", "output directory");
DEFINE_uint32(p, 8, "partition of the graph");

using namespace sics::graph;
namespace fs = std::filesystem;

using sics::graph::core::common::BlockID;
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::VertexID;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string root_path = FLAGS_i;
  std::string out_dir = FLAGS_o;

  fs::path dir = out_dir + "graphs";
  if (!fs::exists(dir)) {
    if (!fs::create_directories(dir)) {
      LOGF_FATAL("Failed creating directory: {}", dir.c_str());
    }
  }
  fs::path label_path = out_dir + "label";
  if (!fs::exists(label_path)) {
    if (!fs::create_directories(label_path)) {
      LOGF_FATAL("Failed creating directory: {}", label_path.c_str());
    }
  }

  uint32_t num_vertices = 150;

  // label
  std::ofstream label_file(label_path, std::ios::binary);
  uint32_t label[num_vertices];
  std::fill(label, label + 25, 0);
  std::fill(label + 25, label + 40, 1);
  std::fill(label + 40, label + 45, 2);
  std::fill(label + 45, label + 50, 3);
  std::fill(label + 50, label + 150, 4);
  label_file.write((char*)label, 4 * num_vertices);
  label_file.close();

  // csr file
  uint32_t degrees[num_vertices];
  uint64_t offsets[num_vertices];
  for (uint32_t i = 0; i < num_vertices; i++) {
    degrees[i] = 0;
    offsets[i] = 0;
  }

  // degree
  std::ifstream csv(root_path);
  std::string line;
  while (std::getline(csv, line)) {
    std::stringstream ss(line);
    uint32_t src, dst, type;
    ss >> src >> type >> dst;
    degrees[src] = degrees[src] + 1;
  }

  csv.clear();
  csv.seekg(0, std::ios::beg);
  while (std::getline(csv, line)) {

  }

  // offset
  for (uint32_t i = 0; i < num_vertices; i++) {
  }

  core::data_structures::GraphMetadata graph_metadata;

  YAML::Node meta;
  graph_metadata.set_type("block");
  graph_metadata.set_num_subgraphs(1);
  std::vector<sics::graph::core::data_structures::BlockMetadata> tmp;
  for (BlockID i = 0; i < 1; i++) {
    sics::graph::core::data_structures::BlockMetadata block_metadata;
    block_metadata.bid = i;
    block_metadata.num_vertices = 100;
    block_metadata.num_outgoing_edges = 2;
    block_metadata.begin_id = 2;
    block_metadata.end_id = 2;
    tmp.push_back(block_metadata);
  }
  graph_metadata.set_block_metadata_vec(tmp);
  meta["GraphMetadata"] = graph_metadata;
  std::ofstream meta_out(out_dir + "meta.yaml");
  meta_out << meta;
  meta_out.close();
  LOG_INFO("Finish writing subgraphs info");
  // no need for label files and index files and is_in_graph_bitmap.

  // count border vertices.

  return 0;
}
