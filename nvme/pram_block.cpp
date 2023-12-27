#include <gflags/gflags.h>

#include <filesystem>
#include <fstream>
#include <iostream>

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph_metadata.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_string(out, "/", "output dir for block files");
DEFINE_uint32(step_v, 500000, "vertex step for block");
DEFINE_uint32(p, 8, "parallelism");

using namespace sics::graph;
using sics::graph::core::common::BlockID;
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::VertexID;

namespace fs = std::filesystem;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string root_path = FLAGS_i;
  auto step = FLAGS_step_v;
  auto parallelism = FLAGS_p;
  auto out_dir = FLAGS_out + "/";

  // create directory of out_dir + "/blocks"
  fs::path dir = FLAGS_out;
  if (!fs::exists(dir)) {
    if (!fs::create_directory(dir)) {
      LOGF_FATAL("Failed creating directory: {}", dir);
    }
  }

  // read graph of one subgraph
  core::data_structures::GraphMetadata graph_metadata(root_path);
  if (graph_metadata.get_num_subgraphs() != 1) {
    LOG_FATAL("num subgraphs: ", graph_metadata.get_num_subgraphs());
  }

  auto subgraph = graph_metadata.GetSubgraphMetadata(0);
  auto num_vertices = subgraph.num_vertices;
  auto num_edges = subgraph.num_outgoing_edges;

  std::ifstream meta_file(root_path + "graphs/0.bin", std::ios::binary);
  if (!meta_file) {
    LOG_FATAL("Error opening bin file: ", root_path + "graphs/0.bin");
  }
  meta_file.seekg(0, std::ios::end);
  size_t file_size = meta_file.tellg();
  meta_file.seekg(0, std::ios::beg);
  auto data_all = new char[file_size];
  meta_file.read(data_all, file_size);

  size_t size_index = num_vertices * sizeof(VertexID);
  size_t size_degree = num_vertices * sizeof(VertexID);
  size_t size_offset = num_vertices * sizeof(EdgeIndex);

  auto base_degree = data_all + size_index;
  auto base_offset = base_degree + size_degree;
  auto base_edge = base_offset + size_offset;

  VertexID* degree_addr = (VertexID*)base_degree;
  EdgeIndex* offset_addr = (EdgeIndex*)base_offset;
  VertexID* edge_addr = (VertexID*)base_edge;

  // separate the graph into blocks
  // use no local index, only use block ID and vertex ID
  core::common::ThreadPool thread_pool(parallelism);
  core::common::TaskPackage tasks;

  std::vector<BlockID> block_ids;
  std::vector<VertexID> block_bids;
  std::vector<VertexID> block_eids;
  std::vector<size_t> block_num_edges;
  std::vector<size_t> block_num_vertexes;

  VertexID bid = 0, eid = 0 + step;
  bool flag = true;
  int file_id = 0;
  while (flag) {
    if (eid > graph_metadata.get_num_vertices()) {
      eid = graph_metadata.get_num_vertices();
      flag = false;
    }
    auto task = [bid, eid, out_dir, file_id, degree_addr, offset_addr,
                 edge_addr]() {
      auto size = eid - bid;
      auto offset_new = new EdgeIndex[size];
      auto offset_begin = offset_addr[bid];
      for (int i = 0; i < size; i++) {
        offset_new[i] = offset_addr[bid + i] - offset_begin;
      }

      std::ofstream block_file(out_dir + std::to_string(file_id) + ".bin",
                               std::ios::binary);
      if (!block_file) {
        LOG_FATAL("Error opening bin file: ",
                  out_dir + std::to_string(file_id) + ".bin");
      }
      // degree info
      block_file.write((char*)(degree_addr + bid), sizeof(VertexID) * size);
      // offset info
      block_file.write((char*)offset_new, sizeof(EdgeIndex) * size);
      // edges info
      block_file.write(
          (char*)(edge_addr + offset_addr[bid]),
          sizeof(VertexID) * (offset_new[eid - 1] + degree_addr[eid - 1]));
      block_file.close();
      // write back to disk
    };
    tasks.push_back(task);
    block_ids.push_back(file_id);
    block_bids.push_back(bid);
    block_eids.push_back(eid);
    block_num_edges.push_back(offset_addr[eid - 1] - offset_addr[bid] +
                              degree_addr[eid - 1]);
    block_num_vertexes.push_back(eid - bid);
    // next task round
    bid = eid;
    if (bid >= graph_metadata.get_num_vertices()) break;
    eid += step;
    file_id++;
  }
  thread_pool.SubmitSync(tasks);

  // write meta file
  YAML::Node meta;
  graph_metadata.set_type("block");
  graph_metadata.set_num_blocks(file_id + 1);
  std::vector<sics::graph::core::data_structures::BlockMetadata> tmp;
  for (size_t i = 0; i < block_ids.size(); i++) {
    sics::graph::core::data_structures::BlockMetadata block_metadata;
    block_metadata.bid = block_ids[i];
    block_metadata.begin_id = block_bids[i];
    block_metadata.end_id = block_eids[i];
    block_metadata.num_outgoing_edges = block_num_edges[i];
    block_metadata.num_vertices = block_num_vertexes[i];
    tmp.push_back(block_metadata);
  }
  graph_metadata.set_block_metadata_vec(tmp);
  meta["GraphMetadata"] = graph_metadata;
  std::ofstream meta_file_out(out_dir + "meta.yaml");
  meta_file_out << meta;
  meta_file_out.close();
}