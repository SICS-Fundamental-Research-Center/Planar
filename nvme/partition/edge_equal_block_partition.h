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

namespace fs = std::filesystem;

// Partition the graph into equal-sized partitions based on the number of
// edges. The partitioning is done by reading the graph from the root_path and
// writing the partitions to the output_path.
void EdgeEqualBlockPartition(const std::string& root_path,
                             const std::string& graph_name,
                             const std::string& output_path,
                             const size_t num_partitions) {
  // Load the graph.
  core::data_structures::GraphMetadata graph_metadata(root_path);
  if (graph_metadata.get_num_subgraphs() != 1) {
    LOG_FATAL("num subgraphs: ", graph_metadata.get_num_subgraphs());
  }

  auto subgraph = graph_metadata.GetSubgraphMetadata(0);
  auto num_vertices = subgraph.num_vertices;
  auto num_edges = subgraph.num_outgoing_edges;

  auto step_edge = (num_edges + num_partitions - 1) / num_partitions;

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

  std::vector<BlockID> block_ids;
  std::vector<VertexID> block_bids;
  std::vector<VertexID> block_eids;
  std::vector<size_t> block_num_edges;
  std::vector<size_t> block_num_vertexes;

  BlockID blockID = 0;
  VertexID bid = 0, eid = 0;
  for (; bid < num_vertices;) {
    EdgeIndex step = 0;
    while (step < step_edge && eid < num_vertices) {
      step = offset_addr[eid];
      eid++;
    }
    // get relative info for block
    block_ids.push_back(blockID);
    block_bids.push_back(bid);
    block_eids.push_back(eid);
    block_num_edges.push_back(step);
    block_num_vertexes.push_back(eid - bid);

    // write block info
    std::string block_file =
        output_path + "block_" + std::to_string(blockID) + ".bin";
    std::ofstream block_meta_file(block_file, std::ios::binary);

    // get next block init state
    bid = eid;
    blockID++;
  }
}

}  // namespace sics::graph::nvme::partition

#endif  // GRAPH_SYSTEMS_NVME_PARTITION_EDGE_EQUAL_BLOCK_PARTITION_H_
