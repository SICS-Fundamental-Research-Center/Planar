 #ifndef GRAPH_SYSTEMS_NVME_PARTITION_VERTEX_EQUAL_BLOCK_PARTITION_H_
#define GRAPH_SYSTEMS_NVME_PARTITION_VERTEX_EQUAL_BLOCK_PARTITION_H_

#include <gflags/gflags.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph_metadata.h"

namespace sics::graph::nvme::partition {

using sics::graph::core::common::BlockID;
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::VertexDegree;
using sics::graph::core::common::VertexID;

namespace fs = std::filesystem;

void VertexEqualBlockPartition(const std::string& root_path,
                               uint32_t offset_ratio, uint32_t step_v,
                               uint32_t step_e, std::string& mode) {
  // create directory of out_dir + "/blocks"
  fs::path dir = root_path + "/subBlocks";
  if (!fs::exists(dir)) {
    if (!fs::create_directories(dir)) {
      LOGF_FATAL("Failed creating directory: {}", dir.c_str());
    }
  }
  std::string index_fname = dir.string() + "/index.bin";
  std::string edge_fname = dir.string() + "/edges.bin";
  std::string meta_fname = dir.string() + "/blocks_meta.yaml";

  // read graph of one subgraph
  core::data_structures::GraphMetadata graph_metadata(root_path);
  if (graph_metadata.get_num_subgraphs() != 1) {
    LOG_FATAL("num subgraphs: ", graph_metadata.get_num_subgraphs());
  }
  core::data_structures::BlockMetadata metadata;
  metadata.mode = mode;
  metadata.vertex_size = step_v;
  metadata.edge_size = step_e;
  metadata.num_vertices = graph_metadata.get_num_vertices();
  metadata.num_edges = graph_metadata.get_num_edges();
  uint32_t num_subBlocks = (metadata.num_vertices + step_v - 1) / step_v;
  metadata.num_subBlocks = num_subBlocks;
  metadata.offset_ratio = offset_ratio;

  auto subgraph = graph_metadata.GetSubgraphMetadata(0);
  auto num_vertices = subgraph.num_vertices;
  // auto num_edges = subgraph.num_outgoing_edges;

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

  // create index.bin file
  auto num_offsets = ((num_vertices - 1) / offset_ratio) + 1;
  auto offset_new = new EdgeIndex[num_offsets];
  for (uint32_t i = 0; i < num_offsets; i++) {
    auto index = i * offset_ratio;
    offset_new[i] = offset_addr[index];
  }
  std::ofstream index_file(index_fname, std::ios::binary);
  index_file.write((char*)offset_new, num_offsets * sizeof(EdgeIndex));
  index_file.write((char*)degree_addr, num_vertices * sizeof(VertexDegree));
  index_file.close();
  delete[] offset_new;
  LOGF_INFO("Create index.bin file.");

  // separate the graph into blocks
  // use no local index, only use block ID and vertex ID
  core::common::ThreadPool thread_pool(1);
  core::common::TaskPackage tasks;

  std::vector<core::data_structures::SubBlock> subBlks;

  auto bid = 0;
  auto eid = metadata.num_vertices;
  if (mode == "vertex") {
    auto p = (num_vertices + step_v - 1) / step_v;
    subBlks.resize(p);
    for (uint32_t i = 0; i < p; i++) {
      auto begin_id = i * step_v;
      auto end_id = (i + 1) * step_v;
      auto num_edge = 0;
      if (end_id >= num_vertices) {
        end_id = num_vertices;
        num_edge = offset_addr[end_id - 1] - offset_addr[begin_id] +
                   degree_addr[end_id - 1];
      } else {
        num_edge = offset_addr[end_id] - offset_addr[begin_id];
      }
      subBlks.at(i).id = i;
      subBlks.at(i).begin_id = begin_id + bid;
      subBlks.at(i).end_id = end_id + bid;
      subBlks.at(i).num_edges = num_edge;
      subBlks.at(i).num_vertices = end_id - begin_id;
      subBlks.at(i).begin_offset = offset_addr[begin_id];
    }
  } else if (mode == "edge") {
    // TODO: consider vertex covering two or more edge block
    auto p = ((num_vertices - 1) / step_e) + 1;
    auto size = (num_vertices + p - 1) / p;
    for (uint32_t i = 0; i < p; i++) {
    }
  }
  std::ofstream out_file(edge_fname, std::ios::binary);
  out_file.write((char*)edge_addr, metadata.num_edges * sizeof(VertexID));
  out_file.close();
  delete[] data_all;
  LOGF_INFO("Create edges file for CSR format!");

  // write meta file
  YAML::Node meta;
  metadata.subBlocks = subBlks;
  meta["BlockMetadata"] = metadata;
  std::ofstream meta_file_out(meta_fname);
  meta_file_out << meta;
  meta_file_out.close();
  LOGF_INFO("Create metadata file!");
}

}  // namespace sics::graph::nvme::partition

#endif  // GRAPH_SYSTEMS_NVME_PARTITION_VERTEX_EQUAL_BLOCK_PARTITION_H_
