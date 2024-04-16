#ifndef GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_NEIGHBOR_HOP_H_
#define GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_NEIGHBOR_HOP_H_

#include <fstream>
#include <iostream>
#include <string>

#include "core/common/types.h"
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

  void Init(const std::string& root_path) {
    Read(root_path + "precomputing/one_hop_min.bin", 1);
    Read(root_path + "precomputing/two_hop_min.bin", 3);
  }

  void Read(const std::string& path, int mode) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
      LOGF_FATAL("Cannot open binary file {}", path);
    }
    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    file.seekg(0, std::ios::beg);
    auto num_vertices = size / sizeof(VertexID);
    if (mode == 1) {
      min_one_hop_neighbor = new VertexID[num_vertices];
      file.read(reinterpret_cast<char*>(min_one_hop_neighbor), size);
    } else if (mode == 2) {
      max_one_hop_neighbor = new VertexID[num_vertices];
      file.read(reinterpret_cast<char*>(max_one_hop_neighbor), size);
    } else if (mode == 3) {
      min_two_hop_neighbor = new VertexID[num_vertices];
      file.read(reinterpret_cast<char*>(min_two_hop_neighbor), size);
    } else if (mode == 4) {
      max_two_hop_neighbor = new VertexID[num_vertices];
      file.read(reinterpret_cast<char*>(max_two_hop_neighbor), size);
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
