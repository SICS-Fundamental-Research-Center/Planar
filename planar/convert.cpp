#include <gflags/gflags.h>

#include <algorithm>
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
  fs::path label_path = out_dir + "label.bin";

  LOG_INFO("begin convert");

  const uint32_t num_vertices = 50000100;
  // label
  std::ofstream label_file(label_path, std::ios::binary);
  auto label = new uint32_t[num_vertices];
  std::fill(label, label + 25000000, 0);
  std::fill(label + 25000000, label + 40000000, 1);
  std::fill(label + 40000000, label + 45000000, 2);
  std::fill(label + 45000000, label + 50000000, 3);
  std::fill(label + 50000000, label + 50000100, 4);
  label_file.write((char*)label, 4 * num_vertices);
  label_file.close();
  delete[] label;

  LOG_INFO("label write finish!");

  // csr file
  auto degrees = new uint32_t[num_vertices];
  auto offsets = new uint64_t[num_vertices];
  auto index = new uint64_t[num_vertices];
  for (uint32_t i = 0; i < num_vertices; i++) {
    degrees[i] = 0;
    offsets[i] = 0;
    index[i] = 0;
  }

  // Degree
  std::ifstream csv(root_path);
  std::string line;
  uint64_t num_edges = 0;
  while (std::getline(csv, line)) {
    std::stringstream ss(line);
    uint32_t src, dst, type;
    ss >> src >> dst;
    degrees[src] = degrees[src] + 1;
    num_edges++;
  }

  LOG_INFO("count degrees");

  // Offset
  offsets[0] = 0;
  for (uint32_t i = 1; i < num_vertices; i++) {
    offsets[i] = offsets[i - 1] + degrees[i - 1];
  }

  LOG_INFO("count offset");

  csv.clear();
  csv.seekg(0, std::ios::beg);

  auto edges = new VertexID[num_edges];
  while (std::getline(csv, line)) {
    std::stringstream ss(line);
    uint32_t src, dst, type;
    ss >> src >> dst;
    auto ptr = edges + offsets[src];
    ptr[index[src]] = dst;
    index[src] += 1;
  }

  LOG_INFO("copy edges");

  // check for edges
  //  for (VertexID i = 0; i < num_vertices; i++) {
  //    auto ptr = edges + offsets[i];
  //    auto d = degrees[i];
  //    std::string tmp = "";
  //    for (int j = 0; j < d; j++) {
  //      tmp = tmp + std::to_string(ptr[j]) + " ";
  //    }
  //    LOGF_INFO("Vertex {} degree {}, edges: {}", i, d, tmp);
  //  }

  std::ofstream out(out_dir + "/graphs/0.bin", std::ios::binary);
  out.write((char*)degrees, sizeof(VertexID) * num_vertices);
  out.write((char*)offsets, sizeof(uint64_t) * num_vertices);
  out.write((char*)edges, sizeof(VertexID) * num_edges);
  out.close();

  LOG_INFO("write edges finish!");

  delete[] degrees;
  delete[] offsets;
  delete[] index;
  delete[] edges;

  core::data_structures::GraphMetadata graph_metadata;
  YAML::Node meta;
  graph_metadata.set_type("block");
  graph_metadata.set_num_vertices(num_vertices);
  graph_metadata.set_num_edges(num_edges);
  graph_metadata.set_max_vid(num_vertices - 1);
  graph_metadata.set_min_vid(0);
  graph_metadata.set_count_border_vertices(0);
  graph_metadata.set_num_subgraphs(1);

  std::vector<sics::graph::core::data_structures::BlockMetadata> tmp;
  sics::graph::core::data_structures::BlockMetadata sub;
  sub.bid = 0;
  sub.num_vertices = num_vertices;
  sub.num_outgoing_edges = num_edges;
  sub.begin_id = 0;
  sub.end_id = num_vertices;
  tmp.push_back(sub);
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
