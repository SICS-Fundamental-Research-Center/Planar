#ifndef GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_INDEX_H_
#define GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_INDEX_H_

#include <yaml-cpp/yaml.h>

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

struct Triangle {
  Triangle() : a(MAX_VERTEX_ID), b(MAX_VERTEX_ID), c(MAX_VERTEX_ID) {}
  VertexID a;
  VertexID b;
  VertexID c;
};

// Star of four neighbors
struct Star4 {
  Star4()
      : center(MAX_VERTEX_ID),
        a(MAX_VERTEX_ID),
        b(MAX_VERTEX_ID),
        c(MAX_VERTEX_ID),
        d(MAX_VERTEX_ID) {}
  Star4(VertexID center, VertexID a, VertexID b, VertexID c, VertexID d)
      : center(center), a(a), b(b), c(c), d(d) {}
  VertexID center;
  VertexID a;
  VertexID b;
  VertexID c;
  VertexID d;
};

// Path of three length, where the middle vertex b only connect to a and c
struct Path {
  Path() : a(MAX_VERTEX_ID), b(MAX_VERTEX_ID), c(MAX_VERTEX_ID) {}
  VertexID a;
  VertexID b;
  VertexID c;
};

struct IndexMeta {
  uint32_t num_triangles;
  uint32_t num_stars4;
  uint32_t num_paths;
};

struct IndexInfo {
  ~IndexInfo() {
    if (triangles) delete[] triangles;
    if (stars4) delete[] stars4;
    if (paths) delete[] paths;
  }

  IndexMeta meta;

  Triangle* triangles = nullptr;
  Star4* stars4 = nullptr;
  Path* paths = nullptr;
};

}  // namespace sics::graph::nvme::data_structures

namespace YAML {

using GraphID = sics::graph::core::common::GraphID;
using BlockID = sics::graph::core::common::BlockID;
using VertexID = sics::graph::core::common::VertexID;
using EdgeIndex = sics::graph::core::common::EdgeIndex;

template <>
struct convert<sics::graph::nvme::data_structures::IndexMeta> {
  static Node encode(
      const sics::graph::nvme::data_structures::IndexMeta& meta) {
    Node node;
    node["num_triangles"] = meta.num_triangles;
    node["num_stars4"] = meta.num_stars4;
    node["num_paths"] = meta.num_paths;
    return node;
  }

  static bool decode(const Node& node,
                     sics::graph::nvme::data_structures::IndexMeta& meta) {
    meta.num_triangles = node["num_triangles"].as<uint32_t>();
    meta.num_stars4 = node["num_stars4"].as<uint32_t>();
    meta.num_paths = node["num_paths"].as<uint32_t>();
    return true;
  }
};

}  // namespace YAML

#endif  // GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_INDEX_H_
