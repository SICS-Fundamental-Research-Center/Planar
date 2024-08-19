#ifndef GRAPH_SYSTEMS_NVME_PARTITION_EDGE_EQUAL_BLOCK_PARTITION_H_
#define GRAPH_SYSTEMS_NVME_PARTITION_EDGE_EQUAL_BLOCK_PARTITION_H_

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
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexIndex;

namespace fs = std::filesystem;

// Partition the graph into equal-sized partitions based on the number of
// edges. The partitioning is done by reading the graph from the root_path and
// writing the partitions to the output_path.
void EdgeEqualBlockPartition(const std::string& root_path,
                             const std::string& out_dir,
                             const size_t num_partitions,
                             const uint32_t parallelism,
                             const uint32_t step_e = 0) {
  // create directory of out_dir + "/blocks"
  fs::path dir = out_dir + "blocks";
  if (!fs::exists(dir)) {
    if (!fs::create_directories(dir)) {
      LOGF_FATAL("Failed creating directory: {}", dir.c_str());
    }
  }
  // Load the graph.
  core::data_structures::GraphMetadata graph_metadata(root_path);
  if (graph_metadata.get_num_subgraphs() != 1) {
    LOG_FATAL("num subgraphs: ", graph_metadata.get_num_subgraphs());
  }

  auto subgraph = graph_metadata.GetSubgraphMetadata(0);
  auto num_vertices = subgraph.num_vertices;
  auto num_edges = subgraph.num_outgoing_edges;

  EdgeIndex step_edge = step_e;
  if (step_e == 0) {
    step_edge = (num_edges + num_partitions - 1) / num_partitions;
  }

  // single thread to split the graph
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

  std::vector<VertexID> block_bids;
  std::vector<VertexID> block_eids;
  std::vector<size_t> block_num_edges;
  std::vector<size_t> block_num_vertexes;

  BlockID block_num = 0;
  VertexID bid = 0, eid = 0;
  LOG_INFO("Begin scanning edges");
  for (; bid < num_vertices;) {
    auto bid_offset = offset_addr[bid];
    EdgeIndex step = bid_offset;
    while (step < bid_offset + step_edge) {
      eid++;
      if (eid == num_vertices) {
        break;
      }
      step = offset_addr[eid];
    }
    // get relative info for block
    block_bids.push_back(bid);
    block_eids.push_back(eid);
    block_num_edges.push_back(step - bid_offset);
    block_num_vertexes.push_back(eid - bid);
    // get next block init state
    block_num++;
    bid = eid;
  }
  block_num_edges.at(block_num - 1) =
      block_num_edges.at(block_num - 1) + degree_addr[num_vertices - 1];
  if (block_num_edges.at(block_num - 1) == 0) {
    auto v_num = block_num_vertexes.at(block_num - 1);
    block_num--;
    block_eids.at(block_num - 1) = block_eids.at(block_num - 1) + v_num;
    block_num_vertexes.at(block_num - 1) =
        block_num_vertexes.at(block_num - 1) + v_num;
  }
  LOG_INFO("Finish scanning edges");

  // write block info by multi thread
  core::common::ThreadPool pool(parallelism);
  core::common::TaskPackage tasks;
  LOG_INFO("Begin writing blocks info");
  for (GraphID i = 0; i < block_num; i++) {
    auto bid = block_bids.at(i);
    auto eid = block_eids.at(i);
    auto num_edge = block_num_edges.at(i);
    auto num_vertex = block_num_vertexes.at(i);
    auto task = [degree_addr, offset_addr, edge_addr, i, bid, eid, num_vertex,
                 num_edge, out_dir]() {
      std::ofstream block_file(out_dir + "blocks/" + std::to_string(i) + ".bin",
                               std::ios::binary);
      if (!block_file) {
        LOG_FATAL("Error opening bin file: ",
                  out_dir + "blocks/" + std::to_string(i) + ".bin");
      }

      auto offset_new = new EdgeIndex[num_vertex];
      for (size_t i = 0; i < num_vertex; i++) {
        offset_new[i] = offset_addr[i + bid] - offset_addr[bid];
      }
      auto degree_begin = degree_addr + bid;
      block_file.write((char*)degree_begin, sizeof(VertexID) * num_vertex);
      // offset info
      block_file.write((char*)offset_new, sizeof(EdgeIndex) * num_vertex);
      // edges info
      auto edge_begin = edge_addr + offset_addr[bid];
      block_file.write((char*)edge_begin, sizeof(VertexID) * num_edge);
      delete[] offset_new;
      block_file.close();
    };
    tasks.push_back(task);
  }
  pool.SubmitSync(tasks);
  meta_file.close();
  LOG_INFO("Finish writing blocks info");

  // write meta file
//  YAML::Node meta;
//  graph_metadata.set_type("block");
//  graph_metadata.set_num_subgraphs(block_num);
//  std::vector<sics::graph::core::data_structures::BlockMetadata> tmp;
//  for (size_t i = 0; i < block_num; i++) {
//    sics::graph::core::data_structures::BlockMetadata block_metadata;
//    block_metadata.bid = i;
//    block_metadata.begin_id = block_bids[i];
//    block_metadata.end_id = block_eids[i];
//    block_metadata.num_outgoing_edges = block_num_edges[i];
//    block_metadata.num_vertices = block_num_vertexes[i];
//    tmp.push_back(block_metadata);
//  }
//  graph_metadata.set_block_metadata_vec(tmp);
//  meta["GraphMetadata"] = graph_metadata;
//  std::ofstream meta_file_out(out_dir + "meta.yaml");
//  meta_file_out << meta;
//  meta_file_out.close();
//  LOG_INFO("Finish writing meta file");
}

// use binary search to find the position of the edge
uint32_t FindPos(EdgeIndex* offset_addr, EdgeIndex begin, EdgeIndex end,
                 EdgeIndex num) {
  auto target = offset_addr[begin] + num;
  auto left = begin, right = end;
  while (left < right) {
    auto mid = left + (right - left) / 2;
    if (offset_addr[mid] < target) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

}  // namespace sics::graph::nvme::partition

#endif  // GRAPH_SYSTEMS_NVME_PARTITION_EDGE_EQUAL_BLOCK_PARTITION_H_
