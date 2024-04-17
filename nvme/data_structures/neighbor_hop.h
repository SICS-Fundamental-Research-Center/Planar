#ifndef GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_NEIGHBOR_HOP_H_
#define GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_NEIGHBOR_HOP_H_

#include <fstream>
#include <iostream>
#include <string>

#include "core/common/types.h"
#include "core/data_structures/graph_metadata.h"
#include "core/util/logging.h"

namespace sics::graph::nvme::data_structures {

using GraphID = core::common::GraphID;
using VertexID = core::common::VertexID;
using VertexIndex = core::common::VertexIndex;
using EdgeIndex = core::common::EdgeIndex;
using VertexDegree = core::common::VertexDegree;

struct NeighborHopInfo {
 public:
  NeighborHopInfo() = default;
  ~NeighborHopInfo() {
    delete[] min_one_hop_neighbor;
    delete[] max_one_hop_neighbor;
    delete[] min_two_hop_neighbor;
    delete[] max_two_hop_neighbor;
  }

  void Init(const std::string& root_path,
            const core::data_structures::GraphMetadata& metadata) {
    min_one_hop_neighbor = new VertexID[metadata.get_num_vertices()];
    min_two_hop_neighbor = new VertexID[metadata.get_num_vertices()];
    for (int i = 0; i < metadata.get_num_blocks(); i++) {
      auto& block = metadata.GetBlockMetadata(i);
      Read(root_path + "precomputing/" + std::to_string(block.bid) +
               "_min_one_hop.bin",
           1, block.begin_id);
      Read(root_path + "precomputing/" + std::to_string(block.bid) +
               "_min_two_hop.bin",
           3, block.begin_id);
    }
  }

  void Read(const std::string& path, int mode, VertexID block_begin_id) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
      LOGF_FATAL("Cannot open binary file {}", path);
    }
    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    file.seekg(0, std::ios::beg);
    auto num_vertices = size / sizeof(VertexID);
    if (mode == 1) {
      file.read(reinterpret_cast<char*>(min_one_hop_neighbor + block_begin_id),
                size);
    } else if (mode == 2) {
      file.read(reinterpret_cast<char*>(max_one_hop_neighbor + block_begin_id),
                size);
    } else if (mode == 3) {
      file.read(reinterpret_cast<char*>(min_two_hop_neighbor + block_begin_id),
                size);
    } else if (mode == 4) {
      file.read(reinterpret_cast<char*>(max_two_hop_neighbor + block_begin_id),
                size);
    }
    file.close();
  }

  VertexID GetMinOneHop(VertexID id) { return min_one_hop_neighbor[id]; }
  VertexID GetMaxOneHop(VertexID id) { return max_one_hop_neighbor[id]; }
  VertexID GetMinTwoHop(VertexID id) { return min_two_hop_neighbor[id]; }
  VertexID GetMaxTwoHop(VertexID id) { return max_two_hop_neighbor[id]; }

 public:
  VertexID* min_one_hop_neighbor = nullptr;
  VertexID* max_one_hop_neighbor = nullptr;
  VertexID* min_two_hop_neighbor = nullptr;
  VertexID* max_two_hop_neighbor = nullptr;
};

}  // namespace sics::graph::nvme::data_structures

#endif  // GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_NEIGHBOR_HOP_H_
