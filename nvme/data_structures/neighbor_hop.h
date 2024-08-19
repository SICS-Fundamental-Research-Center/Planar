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

enum HopType { OneHopMinId = 1, OneHopMaxId, TwoHopMinId, TwoHopMaxId };

// Now the neighbors include vertex itself.
struct NeighborHopInfo {
 public:
  NeighborHopInfo() = default;
  NeighborHopInfo(std::string& root_path,
                  core::data_structures::BlockMetadata* meta)
      : root_path_(root_path), meta_(meta) {}

  ~NeighborHopInfo() {
    delete[] one_hop_min_id_;
    delete[] one_hop_max_id_;
    delete[] two_hop_min_id_;
    delete[] two_hop_max_id_;
  }

  void LoadHopMinIdInfo(std::string& root_path) {
    one_hop_min_id_ = new VertexID[meta_->num_vertices];
    two_hop_max_id_ = new VertexID[meta_->num_vertices];
    Read(root_path + "precomputing/one_hop_min_id.bin", OneHopMinId);
    Read(root_path + "precomputing/two_hop_min_id.bin", TwoHopMinId);
  }

  void Read(const std::string& path, int mode) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
      LOGF_FATAL("Cannot open binary file {}", path);
    }
    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    file.seekg(0, std::ios::beg);
    // auto num_vertices = size / sizeof(VertexID);
    if (mode == OneHopMinId) {
      file.read((char*)one_hop_min_id_, size);
    } else if (mode == OneHopMaxId) {
      file.read((char*)one_hop_max_id_, size);
    } else if (mode == TwoHopMinId) {
      file.read((char*)two_hop_min_id_, size);
    } else if (mode == TwoHopMaxId) {
      file.read((char*)two_hop_max_id_, size);
    }
    file.close();
  }

  VertexID GetOneHopMinId(VertexID id) { return one_hop_min_id_[id]; }
  VertexID GetOneHopMaxId(VertexID id) { return one_hop_max_id_[id]; }
  VertexID GetTwoHopMinId(VertexID id) { return two_hop_min_id_[id]; }
  VertexID GetTwoHopMaxId(VertexID id) { return two_hop_max_id_[id]; }

  // Function used by precomputing.

  void InitBuffer() {
    auto num_vertices = meta_->num_vertices;
    one_hop_min_id_ = new VertexID[num_vertices];
    one_hop_max_id_ = new VertexID[num_vertices];
    two_hop_min_id_ = new VertexID[num_vertices];
    two_hop_max_id_ = new VertexID[num_vertices];
    for (VertexIndex i = 0; i < num_vertices; i++) {
      one_hop_min_id_[i] = MAX_VERTEX_ID;
      one_hop_max_id_[i] = i;
      two_hop_min_id_[i] = MAX_VERTEX_ID;
      two_hop_max_id_[i] = i;
    }
  }

  void SerializeToDisk() {
    auto num_vertices = meta_->num_vertices;
    std::ofstream min_one_hop_file(
        root_path_ + "precomputing/one_hop_min_id.bin", std::ios::binary);
    min_one_hop_file
        .write((char*)one_hop_min_id_, num_vertices * sizeof(VertexID))
        .flush();
    min_one_hop_file.close();
    std::ofstream max_one_hop_file(
        root_path_ + "precomputing/one_hop_max_id.bin", std::ios::binary);
    max_one_hop_file
        .write((char*)one_hop_max_id_, num_vertices * sizeof(VertexID))
        .flush();
    max_one_hop_file.close();
    std::ofstream min_two_hop_file(
        root_path_ + "precomputing/two_hop_min_id.bin", std::ios::binary);
    min_two_hop_file
        .write((char*)two_hop_min_id_, num_vertices * sizeof(VertexID))
        .flush();
    min_two_hop_file.close();
    std::ofstream max_two_hop_file(
        root_path_ + "precomputing/two_hop_max_id.bin", std::ios::binary);
    max_two_hop_file
        .write((char*)two_hop_max_id_, num_vertices * sizeof(VertexID))
        .flush();
    max_two_hop_file.close();
  }

  void UpdateOneHopInfo(VertexID id, VertexID one_hop_neighbor_id) {
    one_hop_min_id_[id] = std::min(one_hop_min_id_[id], one_hop_neighbor_id);
    one_hop_max_id_[id] = std::max(one_hop_max_id_[id], one_hop_neighbor_id);
  }

  void UpdateTwoHopInfo(VertexID id, VertexID two_hop_nerighbor_id) {
    two_hop_min_id_[id] = std::min(two_hop_min_id_[id], two_hop_nerighbor_id);
    two_hop_max_id_[id] = std::max(two_hop_max_id_[id], two_hop_nerighbor_id);
  }

 public:
  std::string root_path_;
  core::data_structures::BlockMetadata* meta_;
  VertexID* one_hop_min_id_ = nullptr;
  VertexID* one_hop_max_id_ = nullptr;
  VertexID* two_hop_min_id_ = nullptr;
  VertexID* two_hop_max_id_ = nullptr;
};

}  // namespace sics::graph::nvme::data_structures

#endif  // GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_NEIGHBOR_HOP_H_
